package depuradordhcp;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author Carlos Rincon
 */
public class DepuradorDHCP {

    /**
     * @param args the command line arguments
     * @throws IOException  
     */
    public static void main(String[] args) throws IOException {
        BufferedReader terminal = new BufferedReader(new InputStreamReader(System.in));
        ManejadorConexiones mc = null;
        String servidor;
        String puerto;
        String usuario;
        String esquema;
        long horaInicio;
        long horaFin;
        int registrosTotales = 0;
        boolean isTest = true;

        Map mapaArgumentos = parsearArgumentos(args, terminal);

        servidor = mapaArgumentos.get("servidor").toString();
        puerto = mapaArgumentos.get("puerto").toString();
        usuario = mapaArgumentos.get("usuario").toString();
        esquema = mapaArgumentos.get("esquema").toString();
        isTest = Boolean.parseBoolean(mapaArgumentos.get("isTest").toString());

        if (isTest) {
            System.out.println("***Modo de simulación, no se borrará nada***");
        }
        System.out.println(servidor + ":" + puerto + "/" + esquema);

        System.out.print("Ingrese el password para '" + usuario + "': ");
        String password = terminal.readLine();

        mc = new ManejadorConexiones(servidor, puerto, usuario, password, esquema);

        if (null == mc || null == mc.getCon()) {
            System.err.println("Hubo un problema al crear la conexión...");
        }

        horaInicio = new Date().getTime();
        List<ProblematicoTO> listaMta = obtenerListaProblematicos(mc, "dhcp_mta");
        List<ProblematicoTO> listaCm = obtenerListaProblematicos(mc, "dhcp_cable_modems");
        List<ProblematicoTO> listaCpe = obtenerListaProblematicos(mc, "dhcp_cpe");
        List<ProblematicoTO> listaFqdn = obtenerListaProblematicos(mc, "dhcp_fqdn");

        
        registrosTotales += eliminarRegistros(mc, listaMta, "dhcp_mta", isTest);
        registrosTotales += eliminarRegistros(mc, listaCm, "dhcp_cable_modems", isTest);
        registrosTotales += eliminarRegistros(mc, listaCpe, "dhcp_cpe", isTest);
        registrosTotales += eliminarRegistros(mc, listaFqdn, "dhcp_fqdn", isTest);
        horaFin = new Date().getTime();
        
        System.out.println("\n******Se borraron: "+registrosTotales+" registro(s) en total *******");

        terminarEjecucion(mc, horaInicio, horaFin, 0);
    }

    private static Map parsearArgumentos(String[] args, BufferedReader terminal) throws IOException {

        Map mapaArgumentos = new HashMap();
        String servidor = "";
        String puerto = "";
        String user = "";
        String esquema = "";
        boolean isTest = false;

        for (int i = 0; i < args.length; i++) {
            String elemento = args[i];

            if ("-s".equals(elemento)) {

                if (i + 1 >= args.length) {
                    imprimirAyuda("No ha especificado el valor para -s de manera válida");
                    terminarEjecucion(null, 0, 0, 2);
                } else {
                    servidor = args[++i];
                }

            } else if ("-p".equals(elemento)) {

                if (i + 1 >= args.length) {
                    imprimirAyuda("No ha especificado el valor para -p de manera válida");
                    terminarEjecucion(null, 0, 0, 2);
                } else {
                    puerto = args[++i];
                }

            } else if ("-u".equals(elemento)) {

                if (i + 1 >= args.length) {
                    imprimirAyuda("No ha especificado el valor para -u de manera válida");
                    terminarEjecucion(null, 0, 0, 2);
                } else {
                    user = args[++i];
                }

            } else if ("-e".equals(elemento)) {

                if (i + 1 >= args.length) {
                    imprimirAyuda("No ha especificado el valor para -e de manera válida");
                    terminarEjecucion(null, 0, 0, 2);
                } else {
                    esquema = args[++i];
                }

            } else if ("--test".equals(elemento)) {
                isTest = true;

            } else if ("--help".equals(elemento)) {
                imprimirAyuda(null);
                terminarEjecucion(null, 0, 0, 0);

            } else {
                imprimirAyuda("No se reconoce la opción '" + elemento + "'");
                terminarEjecucion(null, 0, 0, 3);
            }
        }

        if ("".equals(servidor)) {
            System.out.print("Ingrese la dirección del servidor: ");
            servidor = terminal.readLine();
        }

        if ("".equals(puerto)) {
            System.out.print("Ingrese el puerto del servidor MySQL: ");
            puerto = terminal.readLine();
        }

        if ("".equals(user)) {
            System.out.print("Ingrese el usuario de acceso: ");
            user = terminal.readLine();
        }

        if ("".equals(esquema)) {
            System.out.print("Ingrese el esquema donde se encuentran las tablas: ");
            esquema = terminal.readLine();
        }

        mapaArgumentos.put("servidor", servidor);
        mapaArgumentos.put("puerto", puerto);
        mapaArgumentos.put("usuario", user);
        mapaArgumentos.put("esquema", esquema);
        mapaArgumentos.put("isTest", isTest);

        return mapaArgumentos;

    }

    @SuppressWarnings("CallToThreadDumpStack")
    private static List<ProblematicoTO> obtenerListaProblematicos(ManejadorConexiones mc, String tabla) {
        List<ProblematicoTO> listaProblematicos = new ArrayList<ProblematicoTO>();

        Connection con = mc.getCon();
        PreparedStatement ps = null;
        ResultSet rs = null;
        String queryRegistrosRepetidos = "";

        if ("dhcp_mta".equals(tabla)) {
            queryRegistrosRepetidos = Queries.CONSULTAR_MAC_REGISTROS_REPETIDOS_DHCP_MTA;

        } else if ("dhcp_cable_modems".equals(tabla)) {
            queryRegistrosRepetidos = Queries.CONSULTAR_MAC_REGISTROS_REPETIDOS_DHCP_CABLE_MODEMS;

        } else if ("dhcp_cpe".equals(tabla)) {
            queryRegistrosRepetidos = Queries.CONSULTAR_MAC_REGISTROS_REPETIDOS_DHCP_CPE;

        } else {
            queryRegistrosRepetidos = Queries.CONSULTAR_MAC_REGISTROS_REPETIDOS_DHCP_FQDN;
        }

        System.out.println("QUERY::::" + queryRegistrosRepetidos);

        try {
            ps = con.prepareStatement(queryRegistrosRepetidos);
            rs = ps.executeQuery();

            while (rs.next()) {
                String mac = "";

                if ("dhcp_mta".equals(tabla)) {
                    mac = rs.getString("MTA_MAC");

                } else if ("dhcp_cable_modems".equals(tabla)) {
                    mac = rs.getString("CAMO_MAC");

                } else if ("dhcp_cpe".equals(tabla)) {
                    mac = rs.getString("CPE_MAC");

                } else {
                    mac = rs.getString("FQDN_MAC");
                }

                String cuenta = rs.getString("CUENTA");

                if ("1".equals(cuenta)) {
                    continue;
                }
                listaProblematicos.add(new ProblematicoTO(mac, cuenta));
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            ManejadorConexiones.cerrarConexiones(rs, ps, null);
        }

        if (!listaProblematicos.isEmpty()) {
            listaProblematicos = ordenarListaProblematicosCuentaDesc(listaProblematicos);
        }

        return listaProblematicos;
    }

    private static void terminarEjecucion(ManejadorConexiones mc, long horaInicio, long horaFinal, int estado) {
        if (null != mc) {
            ManejadorConexiones.cerrarConexiones(null, null, mc.getCon());
        }
        double segundos = (double) ((double) (horaFinal - horaInicio)) / 1000;
        System.out.println("La ejecución tardó: " + segundos + " seg.");
        System.exit(estado);
    }

    private static List<ProblematicoTO> ordenarListaProblematicosCuentaDesc(List<ProblematicoTO> listaProblematicos) {
        Comparator<ProblematicoTO> c = new Comparator<ProblematicoTO>() {

            @Override
            public int compare(ProblematicoTO o1, ProblematicoTO o2) {
                int o1Cuenta = Integer.parseInt(o1.getCantidad());
                int o2Cuenta = Integer.parseInt(o2.getCantidad());
                return o2Cuenta - o1Cuenta;
            }
        };

        Collections.sort(listaProblematicos, c);
        return listaProblematicos;
    }

    private static void imprimirListaProblematicos(List<ProblematicoTO> lista) {
        System.out.println("");

        for (ProblematicoTO p : lista) {
            System.out.println(p.getMac() + " : " + p.getCantidad());
        }

        System.out.println("");
    }

    @SuppressWarnings("CallToThreadDumpStack")
    private static int eliminarRegistros(ManejadorConexiones mc, List<ProblematicoTO> lista, String tabla, boolean isTest) {
        Connection con = mc.getCon();
        PreparedStatement ps = null;
        ResultSet rs = null;
        String query = "";
        int registrosTotales = 0;

        if ("dhcp_mta".equals(tabla)) {
            query = Queries.CONSULTAR_ID_REGISTROS_REPETIDOS_DHCP_MTA;

        } else if ("dhcp_cable_modems".equals(tabla)) {
            query = Queries.CONSULTAR_ID_REGISTROS_REPETIDOS_DHCP_CABLE_MODEMS;

        } else if ("dhcp_cpe".equals(tabla)) {
            query = Queries.CONSULTAR_ID_REGISTROS_REPETIDOS_DHCP_CPE;

        } else {
            query = Queries.CONSULTAR_ID_REGISTROS_REPETIDOS_DHCP_FQDN;
        }

        System.out.println("QUERY::::" + query);

        try {
            ps = con.prepareStatement(query);

            for (ProblematicoTO p : lista) {
                System.out.println("-------" + p.getMac() + "-------");
                ps.setString(1, p.getMac());
                rs = ps.executeQuery();

                int registro = 1;

                while (rs.next()) {
                    String id = "";

                    if ("dhcp_mta".equals(tabla)) {
                        id = rs.getString("ID_DHCP_MTA");

                    } else if ("dhcp_cable_modems".equals(tabla)) {
                        id = rs.getString("id_dhcp_cable_modems");

                    } else if ("dhcp_cpe".equals(tabla)) {
                        id = rs.getString("id_dhcp_cpe");

                    } else {
                        id = rs.getString("id_dhcp_fqdn");
                    }

                    if (1 == registro) {
                        System.out.println("NO SE BORRARÁ: " + id);
                    } else {
                        System.out.println("SE BORRARÁ: " + id);

                        // PreparedStatement para borrar y ejecución
                        String queryBorrado = "";

                        if ("dhcp_mta".equals(tabla)) {
                            queryBorrado = Queries.BORRAR_REGISTRO_POR_MAC_MTA;

                        } else if ("dhcp_cable_modems".equals(tabla)) {
                            queryBorrado = Queries.BORRAR_REGISTRO_POR_MAC_CM;

                        } else if ("dhcp_cpe".equals(tabla)) {
                            queryBorrado = Queries.BORRAR_REGISTRO_POR_MAC_CPE;

                        } else {
                            queryBorrado = Queries.BORRAR_REGISTRO_POR_MAC_FQDN;
                        }

                        System.out.println("QUERY:::::::" + queryBorrado);

                        if (!isTest) {
                            PreparedStatement psBorrar = con.prepareStatement(queryBorrado);
                            psBorrar.setInt(1, Integer.parseInt(id));
                            psBorrar.execute();
                            ManejadorConexiones.cerrarConexiones(null, psBorrar, null);
                        }
                    }
                    registro++;
                }
                System.out.println("---------Se Borraron: " + (registro - 2) + " registro(s)-----------\n\n");
                registrosTotales += (registro - 2);
            }
        } catch (Exception e) {
            e.printStackTrace();

        } finally {
            ManejadorConexiones.cerrarConexiones(rs, ps, null);
        }
        return registrosTotales;
    }

    private static void imprimirAyuda(String cadenaEspecificacion) {

        if (null != cadenaEspecificacion) {
            System.out.println(cadenaEspecificacion);
        }

        System.out.println("DepuradorDHCP [-s servidor] [-p puerto] [-u usuario] [-e esquema] [--help] [--test]");
        System.out.println("\t-s\tDirección del servidor");
        System.out.println("\t-p\tPuerto del servidor MySQL");
        System.out.println("\t-u\tUsuario de acceso al servidor MySQL");
        System.out.println("\t-e\tEsquema donde se encuentran las tablas de DHCP");
        System.out.println("\t--test\tActiva el modo de simulación donde no se borrará nada");
        System.out.println("\t--help\tMuestra esta ayuda");
    }
}
