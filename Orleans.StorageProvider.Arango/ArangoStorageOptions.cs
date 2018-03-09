using System;
using ArangoDB.Client;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Orleans.Runtime;

namespace Orleans.StorageProvider.Arango
{
    public class ArangoStorageOptions
    {
        public string DatabaseName { get; set; } = "Orleans";
        public string Url { get; set; } = "http://localhost:8529";
        public string Username { get; set; }

        [Redact]
        public string Password { get; set; }

        public bool WaitForSync { get; set; } = true;
        public string CollectionName { get; set; }

        /// <summary>
        /// Stage of silo lifecycle where storage should be initialized.  Storage must be initialzed prior to use.
        /// </summary>
        public int InitStage { get; set; } = DEFAULT_INIT_STAGE;
        public const int DEFAULT_INIT_STAGE = ServiceLifecycleStage.ApplicationServices;
    }

    /// <summary>
    /// Configuration validator for ArangoStorageOptions
    /// </summary>
    public class ArangoGrainStorageOptionsValidator : IConfigurationValidator
    {
        private readonly ArangoStorageOptions Options;
        private readonly string Name;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="options">The option to be validated.</param>
        /// <param name="name">The option name to be validated.</param>
        public ArangoGrainStorageOptionsValidator(ArangoStorageOptions options, string name)
        {
            Options = options;
            Name = name;
        }

        public void ValidateConfiguration()
        {
            if (string.IsNullOrWhiteSpace(Options.DatabaseName))
                throw new OrleansConfigurationException(
                    $"Configuration for ArangoGrainStorage {this.Name} is invalid. {nameof(Options.DatabaseName)} is not valid.");

            if (string.IsNullOrWhiteSpace(Options.Url))
                throw new OrleansConfigurationException(
                    $"Configuration for ArangoGrainStorage {this.Name} is invalid. {nameof(Options.Url)} is not valid.");

            if (string.IsNullOrWhiteSpace(Options.Username))
                throw new OrleansConfigurationException(
                    $"Configuration for ArangoGrainStorage {this.Name} is invalid. {nameof(Options.Username)} is not valid.");

            if (string.IsNullOrWhiteSpace(Options.Password))
                throw new OrleansConfigurationException(
                    $"Configuration for ArangoGrainStorage {this.Name} is invalid. {nameof(Options.Password)} is not valid.");
        }
    }
}
