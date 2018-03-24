using System;
using System.Linq;

namespace EastFive.Azure.Storage.Backup.Blob
{
    public struct IntervalCalculator
    {
        private int basis;
        private int cycles;
        private TimeSpan? nextInterval;

        public static readonly IntervalCalculator Default = new IntervalCalculator();

        public IntervalCalculator Concat(TimeSpan lastInterval, int[] data)
        {
            var zeroBasis = data.Count(x => x == 0);
            var dataCycles = data.Sum();
            var dataBasis = data.Length - zeroBasis;
            var lastSeconds = lastInterval.TotalSeconds;
            if (dataCycles != 0 && dataCycles == dataBasis)
                lastSeconds *= 0.9; // reduce by 10 % to allow performance to improve over time when the interval hasn't changed.
            return new IntervalCalculator
            {
                cycles = this.cycles + dataCycles,
                basis = this.basis + dataBasis,
                nextInterval = dataBasis > 0 ? TimeSpan.FromSeconds(dataCycles * lastSeconds / dataBasis) : this.nextInterval
            };
        }

        public IntervalCalculator Concat(IntervalCalculator calc)
        {
            return new IntervalCalculator
            {
                basis = this.basis + calc.basis,
                cycles = this.cycles + calc.cycles,
                nextInterval = calc.nextInterval
            };
        }

        public TimeSpan GetNextInterval(TimeSpan lower, TimeSpan higher)
        {
            // before data exists, default to the median of the range
            var result = nextInterval.HasValue ? nextInterval.Value : TimeSpan.FromSeconds((higher - lower).TotalSeconds / 2);
            if (result > higher)
                return higher;
            if (result < lower)
                return lower;
            return result;
        }
    }
}
