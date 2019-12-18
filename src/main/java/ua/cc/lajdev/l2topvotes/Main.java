package ua.cc.lajdev.l2topvotes;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Properties;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.SpringBootConfiguration;

@SpringBootConfiguration
public class Main {

	private static Logger log = LogManager.getLogger(Main.class);

	private static String connectionUrl;
	private static String dbLogin;
	private static String dbPassword;
	private static Properties properties = new Properties();

	static {

		try {
			Class.forName("com.mysql.cj.jdbc.Driver");
			properties.load(new FileInputStream("votes.properties"));
			connectionUrl = properties.getProperty("db.url");
			dbLogin = properties.getProperty("db.login");
			dbPassword = properties.getProperty("db.password");
		} catch (ClassNotFoundException | IOException e) {
			if (e instanceof ClassNotFoundException)
				log.error("Cannot load driver", e);

			if (e instanceof IOException)
				log.error("Not found property file", e);
		}

	}

	public static void main(String[] args) throws IOException, ParseException, InterruptedException {

//		SpringApplication.run(Main.class, args);

		try (Connection connection = DriverManager.getConnection(connectionUrl, dbLogin, dbPassword);
				BufferedReader in = new BufferedReader(
						new InputStreamReader(new URL(properties.getProperty("votes.url")).openStream()));) {
			String date;
			String charName;
			String inputLine = null;
			boolean isReadedFirstLine = false;

			while ((inputLine = in.readLine()) != null) {
				if (isReadedFirstLine == false) {
					isReadedFirstLine = true;
					continue;
				} else {
					date = inputLine.substring(0, 19);
					charName = inputLine.substring(20).trim();

					if (isRecordsPresent(date, charName, connection))
						continue;
					else {
						int charId = getIdByName(charName, connection);

						if (charId != 0) {
							addReward(charId, connection);
							insertRecord(date, charName, connection);
							log.info(charName + " received an award at " + LocalDate.now() + " " + LocalTime.now());
						} else
							continue;
					}
				}
			}
		} catch (SQLException | IOException e) {
			e.printStackTrace();
		}

	}

	private static boolean isRecordsPresent(String date, String name, Connection connection) {

		try (Statement statement = connection.createStatement();) {
			ResultSet rs = statement
					.executeQuery("SELECT * FROM votes WHERE date = '" + date + "' AND char_name = '" + name + "'");
			if (rs.next()) {
				return true;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return false;

	}

	private static void insertRecord(String date, String name, Connection connection) {

		try (PreparedStatement statement = connection
				.prepareStatement("INSERT INTO votes (date, char_name) VALUES (?,?)");) {
			statement.setString(1, date);
			statement.setString(2, name);
			statement.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	private static int getIdByName(String name, Connection connection) {

		try (Statement statement = connection.createStatement();) {
			ResultSet rs = statement.executeQuery("SELECT * FROM characters WHERE char_name = '" + name + "'");
			if (rs.next())
				return rs.getInt(2);
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return 0;

	}

	private static void addReward(int charId, Connection connection) {

		try (PreparedStatement statement = connection
				.prepareStatement("INSERT INTO items (owner_id, object_id, item_id, count, loc) VALUES (?,?,?,?,?)");) {
			statement.setInt(1, charId);
			statement.setInt(2, new Random().nextInt(900000000));
			statement.setInt(3, 4356);
			statement.setInt(4, new Random().nextInt(10));
			statement.setString(5, "INVENTORY");
			statement.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

}
