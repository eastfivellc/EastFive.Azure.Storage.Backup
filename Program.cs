using System.ServiceProcess;

namespace EastFive.Azure.Storage.Backup
{
    static class Program
    {
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        static void Main()
        {
            ServiceBase[] ServicesToRun;
            ServicesToRun = new ServiceBase[]
            {
                new EastFiveAzureStorageBackupService()
            };
            ServiceBase.Run(ServicesToRun);
        }
    }
}
