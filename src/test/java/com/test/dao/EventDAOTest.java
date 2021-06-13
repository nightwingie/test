package com.test.dao;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.test.model.EventDTO;

public class EventDAOTest {

	@Test
	public void chunkInsertAndGetAndDelete() {
		EventDAO dao = new EventDAO();

		EventDTO event1 = new EventDTO("123", "my_host1", "my_type1", 10L);
		EventDTO event2 = new EventDTO("456", "my_host2", "my_type2", 100L);
		EventDTO event3 = new EventDTO("789", "my_host3", "my_type3", 100L);
		List<EventDTO> list = new ArrayList<>();
		list.add(event1);
		list.add(event2);
		list.add(event3);

		dao.setChunSize(2);
		dao.save(list);

		assertNotNull(dao.getEvent("123"));
		assertNotNull(dao.getEvent("456"));
		assertNotNull(dao.getEvent("789"));

		dao.deleteEvent("123");
		dao.deleteEvent("456");
		dao.deleteEvent("789");

		assertNull(dao.getEvent("123"));
		assertNull(dao.getEvent("456"));
		assertNull(dao.getEvent("789"));
	}
}
