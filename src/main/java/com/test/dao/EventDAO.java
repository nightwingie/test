package com.test.dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.test.log.LogProcessor;
import com.test.model.EventDTO;

public class EventDAO {
	private static final int DEFAULT_CHUNK_SIZE = 500;
	private static final Logger logger = LoggerFactory.getLogger(EventDAO.class);

	private int chunSize;

	public EventDAO() {
		this(DEFAULT_CHUNK_SIZE);
	}
	public EventDAO(int chunSize) {
		this.chunSize = chunSize;
		try {
			Class.forName("org.hsqldb.jdbc.JDBCDriver");
		} catch (ClassNotFoundException e) {
			logger.error("unable to load driver", e);
		}
	}

	public void setChunSize(int chunSize) {
		this.chunSize = chunSize;
	}

	public static <T> Collection<List<T>> prepareChunks(List<T> inputList, int chunkSize) {
	    AtomicInteger counter = new AtomicInteger();
	    return inputList.stream().collect(Collectors.groupingBy(it -> counter.getAndIncrement() / chunkSize)).values();
	}

	public void save(List<EventDTO> events) {
		prepareChunks(events, chunSize).forEach(chunk -> batchInsert(chunk));
	}

	private void batchInsert(List<EventDTO> events) {
		logger.debug(String.format("start saving events, size [%d]", events.size()));
		Connection c = null;
		PreparedStatement ps = null;
		try {
			c = DriverManager.getConnection("jdbc:hsqldb:file:testDB", "SA", "");
			ps = c.prepareStatement("INSERT INTO TEST.LOG_EVENTS (ID, DURATION, HOST, TYPE, ALERT) VALUES ( ?, ?, ?, ?, ?)");

			for (EventDTO event : events) {
				ps.setString(1, event.getId());
				ps.setLong(2, event.getDuration());
				ps.setString(3, event.getHost());
				ps.setString(4, event.getType());
				ps.setBoolean(5, event.getDuration() > LogProcessor.MAX_EVENT_DURATION);
				ps.addBatch();
			}

			int[] rows = ps.executeBatch();
			c.commit();
			logger.info(String.format("finished saving events, rows inserted [%d]", rows == null ? 0 : IntStream.of(rows).sum()));
		} catch (SQLException e) {
			logger.error("failed to save to DB: ", e);
			if (c != null) try { c.rollback(); } catch (SQLException e1) { logger.error("rollback failed:", e); }
		} finally {
			if (ps != null) try { ps.close(); } catch (SQLException e1) { logger.error("error closing PreparedStatement:", e1); }
			if (c != null) try { c.close(); } catch (SQLException e1) { logger.error("error closing Connection:", e1); }
		}
	}

	public EventDTO getEvent(String id) {
		logger.debug(String.format("get event by id: [%s]", id));
		Connection c = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			c = DriverManager.getConnection("jdbc:hsqldb:file:testDB", "SA", "");
			ps = c.prepareStatement("SELECT ID, DURATION, HOST, TYPE, ALERT FROM TEST.LOG_EVENTS WHERE ID=?");
			ps.setString(1, id);
			rs = ps.executeQuery();
			if (rs.next()) {
				return new EventDTO(rs.getString("ID"), rs.getString("TYPE"), rs.getString("HOST"), rs.getLong("DURATION"));
			}
		} catch (SQLException e) {
			logger.error("failed to save to DB: ", e);
			if (c != null) try { c.rollback(); } catch (SQLException e1) { logger.error("rollback failed:", e); }
		} finally {
			if (rs != null) try { rs.close(); } catch (SQLException e1) { logger.error("error closing PreparedStatement:", e1); }
			if (ps != null) try { ps.close(); } catch (SQLException e1) { logger.error("error closing PreparedStatement:", e1); }
			if (c != null) try { c.close(); } catch (SQLException e1) { logger.error("error closing Connection:", e1); }
		}
		return null;
	}

	public void deleteEvent(String id) {
		logger.debug(String.format("delete event by id: [%s]", id));
		Connection c = null;
		PreparedStatement ps = null;
		try {
			c = DriverManager.getConnection("jdbc:hsqldb:file:testDB", "SA", "");
			ps = c.prepareStatement("DELETE FROM TEST.LOG_EVENTS WHERE ID=?");
			ps.setString(1, id);
			int rows = ps.executeUpdate();
			c.commit();
			logger.info(String.format("finished deleting event, rows deleted [%d]", rows));
		} catch (SQLException e) {
			logger.error("failed to save to DB: ", e);
			if (c != null) try { c.rollback(); } catch (SQLException e1) { logger.error("rollback failed:", e); }
		} finally {
			if (ps != null) try { ps.close(); } catch (SQLException e1) { logger.error("error closing PreparedStatement:", e1); }
			if (c != null) try { c.close(); } catch (SQLException e1) { logger.error("error closing Connection:", e1); }
		}
	}
}
