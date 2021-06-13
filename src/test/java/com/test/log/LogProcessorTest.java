package com.test.log;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.test.dao.EventDAO;
import com.test.model.EventDTO;
import com.test.model.LogEntry;

public class LogProcessorTest {

	@Test
	public void stringCompare() {
		LogProcessor lp = new LogProcessor();
		assertTrue(lp.equalOrBothNull(null, null));
		assertTrue(lp.equalOrBothNull("123", "123"));
		assertFalse(lp.equalOrBothNull("123", "456"));
	}

	@Test
	public void readFile() {
		LogProcessor lp = new LogProcessor();
		Map<String, List<LogEntry>> logs = lp.readLogFile("logfile.txt");
		assertNotNull(logs);
		assertEquals(logs.size(), 3);
	}

	@Test
	public void processAndClean() {
		new LogProcessor().process("logfile.txt");
		EventDAO dao = new EventDAO();
		assertNotNull(dao.getEvent("scsmbstgrb"));
		assertNotNull(dao.getEvent("scsmbstgrc"));
		EventDTO event = dao.getEvent("scsmbstgra");
		assertNotNull(event);
		assertEquals(event.getType(), "APPLICATION_LOG");
		assertEquals(event.getHost(), "12345");
		assertNotNull(event.getDuration());
		assertEquals(event.getDuration().intValue(), 5);

		dao.deleteEvent("scsmbstgra");
		dao.deleteEvent("scsmbstgrb");
		dao.deleteEvent("scsmbstgrc");
	}

	@Test
	public void filter() {
		LogProcessor lp = new LogProcessor();
		Map<String, List<LogEntry>> logs = new HashMap<>();
		List<LogEntry> list = new ArrayList<>();
		LogEntry log1 = new LogEntry();
		log1.setId("123");
		list.add(log1);
		logs.put("123", list);

		List<EventDTO> events1 = lp.filter(logs);
		assertNotNull(events1);
		assertEquals(events1.size(), 0);

		List<LogEntry> list2 = new ArrayList<>();
		LogEntry log2 = new LogEntry();
		log2.setId("456");
		log2.setState("STARTED");
		log2.setTimestamp(1491377495212L);
		list2.add(log2);

		LogEntry log3 = new LogEntry();
		log3.setId("456");
		log3.setState("FINISHED");
		log3.setTimestamp(1491377495219L);
		list2.add(log3);

		logs.put("456", list2);

		List<EventDTO> events2 = lp.filter(logs);
		assertNotNull(events2);
		assertEquals(events2.size(), 1);
		assertEquals(events2.get(0).getId(), "456");

		log3.setHost("bad_host");
		List<EventDTO> events3 = lp.filter(logs);
		assertNotNull(events3);
		assertEquals(events3.size(), 0);

		log3.setHost(null);
		log3.setType("bad type");
		List<EventDTO> events4 = lp.filter(logs);
		assertNotNull(events4);
		assertEquals(events4.size(), 0);

		log3.setType(null);
		log3.setState("STARTED");
		List<EventDTO> events5 = lp.filter(logs);
		assertNotNull(events5);
		assertEquals(events5.size(), 0);

		log3.setState("FINISHED");
		log3.setTimestamp(1L);
		log2.setTimestamp(2L);
		List<EventDTO> events6 = lp.filter(logs);
		assertNotNull(events6);
		assertEquals(events6.size(), 0);
	}

}
