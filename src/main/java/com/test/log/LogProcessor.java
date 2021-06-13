package com.test.log;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.test.dao.EventDAO;
import com.test.model.EventDTO;
import com.test.model.LogEntry;

public class LogProcessor {

	private static final Logger logger = LoggerFactory.getLogger(LogProcessor.class);
	private static final Gson GSON = new GsonBuilder().create();
	public static List<String> LOG_STATE = Arrays.asList(new String[] {"STARTED", "FINISHED"});
	public static final short MAX_EVENT_DURATION = 4;

	private final EventDAO eventDAO;

	public LogProcessor() {
		eventDAO = new EventDAO();
	}

	public Map<String, List<LogEntry>> readLogFile(String file) {
		logger.info(String.format("start reading log file [%s]", file));

		final Map<String, List<LogEntry>> logs = new HashMap<>();
		try {
			Reader reader = new FileReader(file);
			BufferedReader br =new BufferedReader(reader);
			String line;
			while ((line = br.readLine()) != null) {
				LogEntry le = GSON.fromJson(line, LogEntry.class);
				if (le == null || le.getId() == null) {
					logger.warn("invalid log entry: " + line);
				} else {
					logs.computeIfAbsent(le.getId(), x -> new ArrayList<>()).add(le);
				}
			}

			br.close();
		} catch (IOException e) {
			logger.error(String.format("error reading log file [%s]", file) , e);
		}

		logger.info(String.format("finished reading log file [%s], total number of unique log id's [%d]", file, logs.size()));

		return logs;
	}

	protected List<EventDTO> filter(Map<String, List<LogEntry>> logs) {
		Predicate<List<LogEntry>> logFilter = x -> {
			logger.debug(String.format("apply filter on log records, id [%s]", x.get(0).getId()));
			if (x.size() != 2) {
				logger.warn(String.format("id=[%s] invalid number of log entries [%d]", x.get(0).getId(), x.size()));
				return false;
			}
			if (x.get(0).getState() == null || x.get(1).getState() == null || !LOG_STATE.contains(x.get(0).getState()) || !LOG_STATE.contains(x.get(1).getState())) {
				logger.warn(String.format("id=[%s] invalid state(s) [%s] [%s]", x.get(0).getId(), x.get(0).getState()));
				return false;
			}
			if (x.get(0).getState() == x.get(1).getState()) {
				logger.warn(String.format("id=[%s] duplicate state [%s]", x.get(0).getId(), x.get(0).getState()));
				return false;
			}
			if (!equalOrBothNull(x.get(0).getHost(), x.get(1).getHost())) {
				logger.warn(String.format("id=[%s] host mismatch [%s] [%s]", x.get(0).getId(), x.get(0).getHost(), x.get(1).getHost()));
				return false;
			}
			if (!equalOrBothNull(x.get(0).getType(), x.get(1).getType())) {
				logger.warn(String.format("id=[%s] type mismatch [%s] [%s]", x.get(0).getId(), x.get(0).getType(), x.get(1).getType()));
				return false;
			}
			Long time1 = x.get(0).getTimestamp();
			Long time2 = x.get(1).getTimestamp();
			if (time1 == null || time1 <= 0 || time2 == null || time1 <= 0) {
				logger.warn(String.format("id=[%s] invalid timestamp(s) [%s] [%s]", x.get(0).getId(), time1, time2));
				return false;
			}
			if ((time1 > time2 && LOG_STATE.get(0).equals(x.get(0).getState())) ||
					(time2 > time1 && LOG_STATE.get(0).equals(x.get(1).getState()))) {
				logger.warn(String.format("id=[%s] log event timestamp in wrong order [%s]-[%d] [%s]-[%d]", x.get(0).getId(),
						x.get(0).getState(), time1, x.get(1).getState(), time2));
				return false;
			}

			long duration = Math.abs(x.get(0).getTimestamp() - x.get(1).getTimestamp());
			if (duration > MAX_EVENT_DURATION) {
				logger.info(String.format("ALERT: id=[%s] event duration [%d]", x.get(0).getId(), duration));
			}

			return true;
		};

		return logs.entrySet()
				.parallelStream()
				.filter(x -> logFilter.test(x.getValue()))
				.map(y -> new EventDTO(y.getValue().get(0).getId(), y.getValue().get(0).getType(), y.getValue().get(0).getHost(),
						Math.abs(y.getValue().get(0).getTimestamp() - y.getValue().get(1).getTimestamp())))
				.collect(Collectors.toList());
	}

	protected boolean equalOrBothNull(String str1, String str2) {
		if (str1 == null)
			return str2 == null;
		else
			return str1.equals(str2);
	}

	public void process(String file) {
		Map<String, List<LogEntry>> logs = readLogFile(file);
		List<EventDTO> events = filter(logs);
		eventDAO.save(events);
	}

	public static void main(String[] args) {
		if (args == null || args.length != 1)
			logger.error("missing parameter: fileName");
		else
			new LogProcessor().process(args[0]);
	}
}
