package com.imdb.dao;


import javassist.util.proxy.Proxy;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpanders;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.jdbc.bolt.BoltNeo4jResultSet;
import org.neo4j.jdbc.utils.Neo4jInvocationHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;


import java.sql.*;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@Repository
public class MovieDao {


    private static final String TYPECASTING_QUERY = "SELECT titles.genres FROM imdb.name_basics as names " +
            "JOIN imdb.middle_table_join as middle ON names.nconst = middle.id_person " +
            "JOIN imdb.title_basics as titles ON titles.nconst = middle.title_id " +
            "WHERE LOWER(names.\"primaryName\") = ? AND names.\"primaryProfession\" LIKE '%act%'";

    private static final String COINCIDENCE_QUERY = "SELECT titles.\"primaryTitle\" FROM imdb.name_basics as names " +
            "JOIN imdb.middle_table_join as middle ON names.nconst = middle.id_person " +
            "JOIN imdb.title_basics as titles ON titles.nconst = middle.title_id " +
            "WHERE LOWER(names.\"primaryName\") = ? OR LOWER(names.\"primaryName\") = ?  " +
            "GROUP BY titles.\"primaryTitle\" " +
            "HAVING COUNT(middle.title_id) > 1";


    private static final String SIX_DEGREES_QUERY = "\n" +
            " MATCH p=shortestPath(\n" +
            " (bacon:Actor {name:\"Bacon, Kevin (I)\"})-[*]-(blair:Actor {name: ?}))\n" +
            " RETURN length (p) as path";


    @Autowired
    JdbcTemplate jdbcTemplate;

    public MovieDao(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }


    /**
     * Given a query by the user, where he/she provides an actor/actress name,
     * the system should determine if that person has become typecasted (at least half of their work is one genre).
     *
     * @param name of the actor/actress
     * @return TRUE if it's Typecast and FALSE otherwise
     */

    public boolean isTypecasting(String name) throws Exception {

        if (StringUtils.isBlank(name)) {
            throw new Exception("Invalid parameter");
        }

        List<String> results = jdbcTemplate.query(TYPECASTING_QUERY, new Object[]{name.toLowerCase()},
                (resultSet, rowNum) -> resultSet.getString("genres"));


        Map<String, Long> genres = results.stream().filter(Objects::nonNull)
                .map(elem -> elem.split(","))
                .flatMap(elem -> Arrays.stream(elem))
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));


        List<Long> freqGenres = genres.values().stream().filter(x -> x >= Integer.valueOf(results.size() / 2))
                .collect(Collectors.toList());


        return freqGenres.size() == 1;
    }

    /**
     * Given a query by the user, where the input is two actors/actresses names,
     * the application replies with a list of movies or TV shows that both people have shared.
     *
     * @param person1 name of actor/actress
     * @param person2 name of actor/actress
     * @return list of shared movies/TV shows
     */
    public List<String> findCoincidence(String person1, String person2) throws Exception {

        if (StringUtils.isBlank(person1)|| StringUtils.isBlank(person2)) {
            throw new Exception("Invalid parameter");
        }


        List<String> listMovies = jdbcTemplate.query(COINCIDENCE_QUERY, new Object[]{person1.toLowerCase(), person2.toLowerCase()},
                (resultSet, rowNum) -> resultSet.getString("primaryTitle"));
        ;

        return listMovies;
    }

    /**
     * Six degrees of Kevin Bacon: Given a query by the user, you must provide what’s the degree of separation
     * between the person (e.g. actor or actress) the user has entered and Kevin Bacon.
     * @param name of the actor/actress
     * @return Number of degree separation with Kevin Bacon
     */
    public int sixDegrees(String name) {
        int degrees = 0;

        try {
            Connection con = DriverManager.getConnection(
                    "jdbc:neo4j:bolt://localhost/?user=neo4j,password=root,scheme=basic");

            PreparedStatement preparedStatement = con.prepareStatement(SIX_DEGREES_QUERY);
            preparedStatement.setString(1, name);

            ResultSet rs = preparedStatement.executeQuery();
            rs.next();
            degrees = rs.getInt("path") / 2;

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return degrees;
    }
}
