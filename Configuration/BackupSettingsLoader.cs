using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;

namespace EastFive.Azure.Storage.Backup.Configuration
{
    public class BackupSettingsLoader
    {
        private readonly string configPath;

        public BackupSettingsLoader(string configPath)
        {
            this.configPath = configPath;
            Load();
        }

        public BackupSettings? Settings { get; private set; }

        public string Error { get; private set; }

        public void OnChanged(object source, FileSystemEventArgs e)
        {
            Load();
        }

        private void Load()
        {
            EastFiveAzureStorageBackupService.Log.Info($"loading config from {configPath}");
            string text;
            using (var fs = new FileStream(configPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            using (var reader = new StreamReader(fs))
                text = reader.ReadToEnd();
            try
            {
                Settings = JsonConvert.DeserializeObject<BackupSettings>(text, new JsonSerializerSettings()
                {
                    Converters = new List<JsonConverter>
                    {
                        new Newtonsoft.Json.Converters.StringEnumConverter()
                    }
                });
                Error = null;
            }
            catch (Exception e)
            {
                Settings = default(BackupSettings?);
                Error = e.Message;
                EastFiveAzureStorageBackupService.Log.Error(e);
            }
        }
    }
}
