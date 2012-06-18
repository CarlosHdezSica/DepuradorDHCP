package depuradordhcp;

/**
 *
 * @author Carlos Rincon
 */
class ProblematicoTO {
    private String mac;
    private String cantidad;

    private ProblematicoTO() {
    }

    public ProblematicoTO(String mac, String cantidad) {
        this.mac = mac;
        this.cantidad = cantidad;
    }
    
    public String getCantidad() {
        return cantidad;
    }

    public void setCantidad(String cantidad) {
        this.cantidad = cantidad;
    }

    public String getMac() {
        return mac;
    }

    public void setMac(String mac) {
        this.mac = mac;
    }
    
    
}
