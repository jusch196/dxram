
package de.hhu.bsinfo.dxram.log;

import de.hhu.bsinfo.utils.Pair;

/**
 * Configuration values for the log service.
 * @author Kevin Beineke <kevin.beineke@hhu.de> 29.03.16
 */
public class LogConfigurationValues {

	/**
	 * Log service configuration attributes
	 */
	public static class Service {
		public static final Pair<String, Boolean> LOG_CHECKSUM = new Pair<String, Boolean>("LogChecksum", true);

		public static final Pair<String, Integer> FLASHPAGE_SIZE = new Pair<String, Integer>("FlashPageSize", 4 * 1024);
		public static final Pair<String, Integer> LOG_SEGMENT_SIZE = new Pair<String, Integer>("LogSegmentSize", 8 * 1024 * 1024);
		public static final Pair<String, Long> PRIMARY_LOG_SIZE = new Pair<String, Long>("PrimaryLogSize", 256 * 1024 * 1024L);
		public static final Pair<String, Long> SECONDARY_LOG_SIZE = new Pair<String, Long>("SecondaryLogSize", 256 * 1024 * 1024L);
		public static final Pair<String, Integer> WRITE_BUFFER_SIZE = new Pair<String, Integer>("WriteBufferSize", 256 * 1024 * 1024);

		public static final Pair<String, Integer> REORG_UTILIZATION_THRESHOLD = new Pair<String, Integer>("ReorgUtilizationThreshold", 70);
	}
}
