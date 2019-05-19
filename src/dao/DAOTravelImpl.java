package dao;

import static dao.DAOUtilities.initPrepQuery;
import static dao.DAOUtilities.silentCloses;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import beans.Travel;

/**
 * <b>La classe DAOTravelImpl se connecte à la base de données</b>
 * <p>
 * Cette classe se charge de récupérer, dans la base, les données voulues
 * </p>
 * @see DAO
 * @see DAOTravel
 * 
 * @author 
 *
 */
public class DAOTravelImpl implements DAOTravel {
	private static final String SQL_DEFAULT	= "SELECT T.id_travel, T.length, T.seat_max, T.luggage_max, C.name, "
								   + "DT.schedule, M.lastname, M.firstname "
								   + "FROM travel T "
								   + "INNER JOIN drives_to DT ON DT.id_travel=T.id_travel "
								   + "INNER JOIN city C ON DT.id_city=C.id_city "
								   + "INNER JOIN drives_with DW ON T.id_travel = DW.id_travel "
								   + "INNER JOIN member M ON DW.id_member = M.id_member ";

	/**
	 * 
	 * Requêtes SQL pour création et suppresion dans la BD
	 * 
	 */
	private static final String SQL_INSERT = "INSERT INTO travel ( length, seat_max, luggage_max) "
										   + "VALUES (?, ?, ?, NOW())";
	private static final String SQL_DELETE = "DELETE FROM travel WHERE id_travel = ?";
	private static final String SQL_FIRST_LAST_CITIES = "    (SELECT  " + 
			"            fc.name " + 
			"        FROM " + 
			"            city fc " + 
			"                INNER JOIN " + 
			"            drives_to D_T ON D_T.id_city = fc.id_city " + 
			"        WHERE " + 
			"            D_T.id_travel = T.id_travel " + 
			"        ORDER BY D_T.schedule ASC " + 
			"        LIMIT 1) AS firstCity, " + 
			"    (SELECT  " + 
			"            lc.name " + 
			"        FROM " + 
			"            city lc " + 
			"                INNER JOIN " + 
			"            drives_to D__T ON D__T.id_city = lc.id_city " + 
			"        WHERE " + 
			"            D__T.id_travel = T.id_travel " + 
			"        ORDER BY D__T.schedule DESC " + 
			"        LIMIT 1) AS lastCity, " ; 
;
	private static final String SQL_BY_ID = "SELECT  " + 
			"    T.id_travel, " + 
			"    T.length, " + 
			"    T.seat_max, " + 
			"    T.luggage_max, " + 
			"    C.name, " + 
			SQL_FIRST_LAST_CITIES +
			"    DT.schedule " + 
			"FROM " + 
			"    travel T " + 
			"        INNER JOIN " + 
			"    drives_to DT ON DT.id_travel = T.id_travel " + 
			"        INNER JOIN " + 
			"    city C ON DT.id_city = C.id_city " + 
			"WHERE " + 
			"    T.id_travel = ? " + 
			"ORDER BY DT.schedule";
	private static final String SQL_SELECT = "SELECT  " + 
			"    T.id_travel, " + 
			"    T.length, " + 
			"    T.seat_max, " + 
			"    T.luggage_max, " + 
			"    C.name, " + 
			"    C.name AS firstCity, " + 
			SQL_FIRST_LAST_CITIES +
		"    DT.schedule, " + 
			"    M.lastname, " + 
			"    M.firstname, " + 
			"    DW.seat " + 
			"FROM " + 
			"    travel T " + 
			"        INNER JOIN " + 
			"    drives_to DT ON DT.id_travel = T.id_travel " + 
			"        INNER JOIN " + 
			"    city C ON DT.id_city = C.id_city " + 
			"        INNER JOIN " + 
			"    drives_with DW ON T.id_travel = DW.id_travel " + 
			"        INNER JOIN " + 
			"    member M ON DW.id_member = M.id_member " + 
			"WHERE " + 
			"    DW.seat = 1 " + 
			"ORDER BY DT.schedule, DW.seat";
	/**
	 * instance de la DAOFactory
	 */
	private DAOFactory daoFactory;
	
	DAOTravelImpl(DAOFactory daoFactory) {
		this.daoFactory = daoFactory;
	}

	/**
	 * création de la fonction de création d'un trajet
	 * @param t
	 * @throws SQLException
	 */
	@Override
	public void create(Travel t) throws DAOException {
		Connection connexion = null;
        PreparedStatement preparedStatement = null;
        ResultSet generatedValues = null;

        try {
            connexion = daoFactory.getConnection();
            preparedStatement = initPrepQuery( connexion, SQL_INSERT, true, t.getLength(), t.getLuggage_max(), t.getSeat_max() );
            int statut = preparedStatement.executeUpdate();
            if ( statut == 0 ) {
                throw new DAOException( "Echec de la création de l'utilisateur, aucune ligne ajoutée dans la table." );
            }
            generatedValues = preparedStatement.getGeneratedKeys();
            if ( generatedValues.next() ) {
                t.setId_travel( generatedValues.getLong( 1 ) );
            } else {
                throw new DAOException( "Echec de la création de l'utilisateur en base, aucun ID auto-généré retourné." );
            }
        } catch ( SQLException e ) {
            throw new DAOException( e );
        } finally {
            silentCloses( generatedValues, preparedStatement, connexion );
        }
	}
	
	@Override
	public Travel find(String cityName) throws DAOException {
		return find(SQL_DEFAULT+" WHERE C.name = ?", cityName);
	}

	@Override
	public Travel find(int id) throws DAOException {
		return find(SQL_BY_ID, id);
	}
	
	private Travel find (String sql, Object... objects) throws DAOException {
        Connection connexion = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        Travel travel = null;
        
        try {
            /* Récupération d'une connexion depuis la Factory */
            connexion = daoFactory.getConnection();
            /*
             * Préparation de la requête avec les objets passés en arguments
             * (ici, uniquement une adresse email) et exécution.
             */
            preparedStatement = initPrepQuery( connexion, sql, true, objects );
            resultSet = preparedStatement.executeQuery();
            /* Parcours de la ligne de données retournée dans le ResultSet */
            if ( resultSet.next() ) {
            	travel = map( resultSet );
            }
        } catch ( SQLException e ) {
            throw new DAOException( e );
        } finally {
            silentCloses( resultSet, preparedStatement, connexion );
        }

		return travel;
	}

	/**
	 * création de la fonction de suppression d'un trajet
	 * @param t
	 * @throws SQLException
	 */
	public void delete(Travel t) throws SQLException {

		Connection connect = null;
		PreparedStatement pstmt = null;

		try {
			connect = daoFactory.getConnection();
			pstmt = initPrepQuery( connect, SQL_DELETE, false, t.getId_travel() );
		} catch ( SQLException e) {
			throw new DAOException( e );
		} finally {
            silentCloses( pstmt, connect );
        }
	}

    /*
     * Simple méthode utilitaire permettant de faire la correspondance (le
     * mapping) entre une ligne issue de la table des utilisateurs (un
     * ResultSet) et un bean Utilisateur.
     */
    private static Travel map( ResultSet rs ) throws SQLException {
		Travel t = new Travel();
		t.setId_travel(rs.getLong("id_travel"));
		t.setDeparture(rs.getString("schedule"));
		t.setFirstCity(rs.getString("firstCity"));
		t.setLastCity(rs.getString("lastCity"));
		t.addSteps(rs.getString("name"), rs.getString("schedule"));
		t.setLength(rs.getLong("length"));
		t.setLuggage_max(rs.getLong("luggage_max"));
		t.setSeat_max(rs.getLong("seat_max"));
        
        return t;
    }

	/**
	 * création d'une liste de trajet
	 * @return
	 * @throws SQLException
	 */
	public ArrayList<Travel> findAll() throws DAOException {

		Connection connect = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		ArrayList<Travel> travel = new ArrayList<Travel>();

		try {
			connect = daoFactory.getConnection();
			pstmt = connect.prepareStatement(SQL_SELECT, ResultSet.TYPE_SCROLL_INSENSITIVE, 
					  ResultSet.CONCUR_READ_ONLY);
			rs = pstmt.executeQuery();
			if (rs != null) {
				//rs.last();
				int count = rowCount(rs);
				rs.beforeFirst();
				int i = 0;
				int j = 0;
				Long id = (long) -1;
				while (rs.next()) {
					if (id != rs.getLong("id_travel") ) {
						if (id != (long) -1) {
							String lastCityName = travel.get(travel.size()-j+1).getLastCity();
							travel.get(travel.size()-j).setLastCity(lastCityName);
							j=0;
						}
						id = rs.getLong("id_travel");
					} else {
						if (travel.size() == (count-1)) {
							String lastCityName = rs.getString("name");
							travel.get(travel.size()-j).setLastCity(lastCityName);
						}
					}
					travel.add(map(rs));
					j++;
					i++;
				}
			}
		}catch ( SQLException e) {
			throw new DAOException( e );
		} finally {
            silentCloses( rs, pstmt, connect );
        }
		return travel;
	}

	private int rowCount(ResultSet rs) {
		int size = 0;
		try {
			while(rs.next()){
			    size++;
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return size;
	}

}
