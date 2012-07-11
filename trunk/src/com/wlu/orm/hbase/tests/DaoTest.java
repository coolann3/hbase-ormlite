package com.wlu.orm.hbase.tests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.wlu.orm.hbase.connection.HBaseConnection;
import com.wlu.orm.hbase.dao.Dao;
import com.wlu.orm.hbase.dao.DaoImpl;
import com.wlu.orm.hbase.exceptions.HBaseOrmException;

import junit.framework.TestCase;

public class DaoTest extends TestCase {

	public void testDao() throws HBaseOrmException {
		HBaseConnection hbaseconnection = new HBaseConnection(
				"hadoop008,hadoop009,hadoop010,hadoop006,hadoop007", "2181", 10);

		Profile p = new Profile("jacky", "14", "Hangzhou, Wen 2 road, #391");
		HashMap<String, String> mp1 = new HashMap<String, String>();
		Map<String, PageContents> mp3 = new HashMap<String, PageContents>();
		List<String> list2 = new ArrayList<String>();
		for (int i = 0; i < 10; i++) {
			mp1.put(String.format("id1%06d", i), "this is page " + i);
			list2.add(String.format("id2%06d", i));
			mp3.put(String.format("id3%06d", i), new PageContents(
					"this is page <type 3> " + i));
		}

		LikePages lp = new LikePages(mp1, list2, mp3);

		User user = new User("1234", p, lp, 8080);

		Dao<User> dao = new DaoImpl<User>(User.class, hbaseconnection);

		dao.Insert(user);
		p = new Profile("hellen", "20", "Beijing, Chaoyang #1");
		user = new User("001", p, null, 8080);
		dao.Insert(user);

		dao.Delete(user, "profile", "address");
		dao.Delete(user, "profile", "aint");
	}

}
