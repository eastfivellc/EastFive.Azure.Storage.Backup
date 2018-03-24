using EastFive.Web;

namespace EastFive.Azure.Storage.Backup.Configuration
{
    public static class AppSettings
    {
        [ConfigKey("Identifies the json formatted file containing the backup settings.  The default is backup.json",
            DeploymentOverrides.Suggested,
            DeploymentSecurityConcern = true,
            Location = "Directory where executable is running",
            PrivateRepositoryOnly = false)]
        public const string ConfigFile = "EastFive.Azure.Storage.Backup.ConfigFile";

        [ConfigKey("The timespan for how often this service wakes up.  The default is 00:30:00 (30 minutes).",
            DeploymentOverrides.Optional,
            DeploymentSecurityConcern = false,
            Location = "N/A",
            PrivateRepositoryOnly = false)]
        public const string WakeUp = "EastFive.Azure.Storage.Backup.WakeUp";
    }
}
