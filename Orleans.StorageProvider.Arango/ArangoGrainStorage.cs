using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using ArangoDB.Client;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Orleans.Storage;
using Orleans.Runtime;
using Orleans.Serialization;

namespace Orleans.StorageProvider.Arango
{
    public class ArangoGrainStorage : IGrainStorage, ILifecycleParticipant<ISiloLifecycle>
    {
        private readonly string Name;
        private readonly ArangoStorageOptions Options;
        private readonly ILoggerFactory LoggerFactory;
        private readonly ILogger Logger;
        private readonly SerializationManager SerializationManager;
        private readonly ITypeResolver TypeResolver;
        private readonly IGrainFactory GrainFactory;

        private Newtonsoft.Json.JsonSerializer JsonSerializerSettings;
        public ArangoDatabase Database {
            get; private set;
         }

        private ConcurrentBag<string> initialisedCollections = new ConcurrentBag<string>();

        public ArangoGrainStorage(string name, ArangoStorageOptions options, ILoggerFactory loggerFactory,
                            SerializationManager serializationManager, ITypeResolver typeResolver, IGrainFactory grainFactory)
        {
            Name = name;
            Options = options;
            LoggerFactory = loggerFactory;
            Logger = loggerFactory.CreateLogger($"{this.GetType().FullName}.{name}");
            SerializationManager = serializationManager;
            TypeResolver = typeResolver;
            GrainFactory = grainFactory;
        }

        public void Participate(ISiloLifecycle lifecycle)
        {
            lifecycle.Subscribe(Options.InitStage, Init, Close);
        }

        public Task Close(CancellationToken ct)
        {
            if (Database != null) {
                Database.Dispose();
                Database = null;
            }
            return Task.CompletedTask;
        }

        public Task Init(CancellationToken ct)
        {
            var grainRefConverter = new GrainReferenceConverter(TypeResolver, GrainFactory);
            JsonSerializerSettings = new JsonSerializer();
            JsonSerializerSettings.Converters.Add(grainRefConverter);

            ArangoDatabase.ChangeSetting(s =>
            {
                s.Database = Options.DatabaseName;
                s.Url = Options.Url;
                s.Credential = new NetworkCredential(Options.Username, Options.Password);
                s.DisableChangeTracking = true;
                s.WaitForSync = Options.WaitForSync;
                s.Serialization.Converters.Add(grainRefConverter);
            });

            this.Database = new ArangoDatabase();

            return Task.CompletedTask;
        }

        private async Task<IDocumentCollection> InitialiseCollection(string name)
        {
            if (!this.initialisedCollections.Contains(name))
            {
                try
                {
                    await this.Database.CreateCollectionAsync(name);
                }
                catch (Exception)
                {
                    Logger.Info($"Arango Storage Provider: Error creating {name} collection, it may already exist");
                }

                this.initialisedCollections.Add(name);
            }

            return this.Database.Collection(name);
        }

        private Task<IDocumentCollection> GetCollection(string grainType)
        {
            if (!string.IsNullOrWhiteSpace(Options.CollectionName))
            {
                return InitialiseCollection(Options.CollectionName);
            }

            return InitialiseCollection(grainType.Split('.').Last().ToArangoCollectionName());
        }

        public async Task ReadStateAsync(string grainType, GrainReference grainReference, IGrainState grainState)
        {
            try
            {
                var primaryKey = grainReference.ToArangoKeyString();
                var collection = await GetCollection(grainType);

                var result = await collection.DocumentAsync<GrainState>(primaryKey).ConfigureAwait(false);
                if (null == result) return;

                if (result.State != null)
                {
                    grainState.State = (result.State as JObject).ToObject(grainState.State.GetType(), JsonSerializerSettings);
                }
                else
                {
                    grainState.State = null;
                }
                grainState.ETag = result.Revision;
            }
            catch (Exception ex)
            {
                Logger.Error(190000, "ArangoStorageProvider.ReadStateAsync()", ex);
                throw new ArangoStorageException(ex.ToString());
            }
        }

        public async Task WriteStateAsync(string grainType, GrainReference grainReference, IGrainState grainState)
        {
            try
            {
                var primaryKey = grainReference.ToArangoKeyString();
                var collection = await GetCollection(grainType);

                var document = new GrainState
                {
                    Id = primaryKey,
                    Revision = grainState.ETag,
                    State = grainState.State
                };

                if (string.IsNullOrWhiteSpace(grainState.ETag))
                {
                    var result = await collection.InsertAsync(document).ConfigureAwait(false);
                    grainState.ETag = result.Rev;
                }
                else
                {
                    var result = await collection.UpdateByIdAsync(primaryKey, document).ConfigureAwait(false);
                    grainState.ETag = result.Rev;
                }
            }
            catch (Exception ex)
            {
                Logger.Error(190001, "ArangoStorageProvider.WriteStateAsync()", ex);
                throw new ArangoStorageException(ex.ToString());
            }
        }

        public async Task ClearStateAsync(string grainType, GrainReference grainReference, IGrainState grainState)
        {
            try
            {
                var primaryKey = grainReference.ToArangoKeyString();
                var collection = await GetCollection(grainType);

                await collection.RemoveByIdAsync(primaryKey).ConfigureAwait(false);

                grainState.ETag = null;
            }
            catch (Exception ex)
            {
                Logger.Error(190002, "ArangoStorageProvider.ClearStateAsync()", ex);
                throw new ArangoStorageException(ex.ToString());
            }
        }
    }

    public static class ArangoGrainStorageFactory
    {
        public static IGrainStorage Create(IServiceProvider services, string name)
        {
            IOptionsSnapshot<ArangoStorageOptions> optionsSnapshot = services.GetRequiredService<IOptionsSnapshot<ArangoStorageOptions>>();
            return ActivatorUtilities.CreateInstance<ArangoGrainStorage>(services, optionsSnapshot.Get(name), name);
        }
    }
}
