package depuradordhcp;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 *
 * @author Carlos Rincon
 */
public class ManejadorConexiones {

    private Connection con;

    @SuppressWarnings("CallToThreadDumpStack")
    public ManejadorConexiones(String servidor, String puerto, String usuario, String password, String esquema) {

        try {
            Class.forName("com.mysql.jdbc.Driver");
            // Step 2: Establish the connection to the database. 
            String url = "jdbc:mysql://" + servidor + ":" + puerto + "/" + esquema;
            con = DriverManager.getConnection(url, usuario, password);

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private ManejadorConexiones() {
    }

    public Connection getCon() {
        return con;
    }

    @SuppressWarnings("CallToThreadDumpStack")
    public static void cerrarConexiones(ResultSet rs, PreparedStatement ps, Connection con) {
        try {
            if (rs != null) {
                rs.close();
            }
            if (ps != null) {
                ps.close();
            }
            if (con != null) {
                con.close();
                System.out.println("Se cerró correctamente la conexión...");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
