using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.Storage;
using Orleans.Providers;
using System.Text.RegularExpressions;

namespace Orleans.StorageProvider.Arango
{
    public static class ArangoStorageSiloBuilderExtension
    {
        /// <summary>
        /// Configure silo to use Arango storage as the default grain storage.
        /// </summary>
        public static ISiloHostBuilder AddArangoGrainStorageAsDefault(this ISiloHostBuilder builder, Action<ArangoStorageOptions> configureOptions)
        {
            return builder.AddArangoGrainStorage(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, configureOptions);
        }

        /// <summary>
        /// Configure silo to use Arango storage for grain storage.
        /// </summary>
        public static ISiloHostBuilder AddArangoGrainStorage(this ISiloHostBuilder builder, string name, Action<ArangoStorageOptions> configureOptions)
        {
            return builder.ConfigureServices(services => services.AddArangoGrainStorage(name, configureOptions));
        }

        /// <summary>
        /// Configure silo to use Arango storage as the default grain storage.
        /// </summary>
        public static ISiloHostBuilder AddArangoGrainStorageAsDefault(this ISiloHostBuilder builder, Action<OptionsBuilder<ArangoStorageOptions>> configureOptions = null)
        {
            return builder.AddArangoGrainStorage(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, configureOptions);
        }

        /// <summary>
        /// Configure silo to use Arango storage for grain storage.
        /// </summary>
        public static ISiloHostBuilder AddArangoGrainStorage(this ISiloHostBuilder builder, string name, Action<OptionsBuilder<ArangoStorageOptions>> configureOptions = null)
        {
            return builder.ConfigureServices(services => services.AddArangoGrainStorage(name, configureOptions));
        }

        /// <summary>
        /// Configure silo to use Arango storage as the default grain storage.
        /// </summary>
        public static IServiceCollection AddArangoGrainStorageAsDefault(this IServiceCollection services, Action<ArangoStorageOptions> configureOptions)
        {
            return services.AddArangoGrainStorage(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, ob => ob.Configure(configureOptions));
        }

        /// <summary>
        /// Configure silo to use Arango storage for grain storage.
        /// </summary>
        public static IServiceCollection AddArangoGrainStorage(this IServiceCollection services, string name, Action<ArangoStorageOptions> configureOptions)
        {
            return services.AddArangoGrainStorage(name, ob => ob.Configure(configureOptions));
        }

        /// <summary>
        /// Configure silo to use Arango storage as the default grain storage.
        /// </summary>
        public static IServiceCollection AddArangoGrainStorageAsDefault(this IServiceCollection services, Action<OptionsBuilder<ArangoStorageOptions>> configureOptions = null)
        {
            return services.AddArangoGrainStorage(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, configureOptions);
        }

        /// <summary>
        /// Configure silo to use Arango storage for grain storage.
        /// </summary>
        public static IServiceCollection AddArangoGrainStorage(this IServiceCollection services, string name,
            Action<OptionsBuilder<ArangoStorageOptions>> configureOptions = null)
        {
            configureOptions?.Invoke(services.AddOptions<ArangoStorageOptions>(name));
            services.AddTransient<IConfigurationValidator>(sp => new ArangoGrainStorageOptionsValidator(sp.GetService<IOptionsSnapshot<ArangoStorageOptions>>().Get(name), name));
            services.ConfigureNamedOptionForLogging<ArangoStorageOptions>(name);
            services.TryAddSingleton(sp => sp.GetServiceByName<IGrainStorage>(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME));
            return services.AddSingletonNamedService(name, ArangoGrainStorageFactory.Create)
                           .AddSingletonNamedService(name, (s, n) => (ILifecycleParticipant<ISiloLifecycle>)s.GetRequiredServiceByName<IGrainStorage>(n));
        }
    }

    internal static class PrivateExtensions
    {
        static Regex documentKeyRegex = new Regex(@"[^a-zA-Z0-9_/-:.@(),=;$!*'%]");

        public static string ToArangoKeyString(this GrainReference grainRef)
        {
            return documentKeyRegex.Replace(grainRef.ToKeyString(), "_");

        }

        static Regex collectionRegex = new Regex(@"[^a-zA-Z0-9_-]");

        public static string ToArangoCollectionName(this string collectionName)
        {
            return documentKeyRegex.Replace(collectionName, "_");
        }
    }
}
