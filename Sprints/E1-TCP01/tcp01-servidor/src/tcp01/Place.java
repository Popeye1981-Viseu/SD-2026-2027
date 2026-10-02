package tcp01;

import java.io.Serializable;

public class Place implements Serializable {          // obrigatório, senão a Person não pode ser enviada
    private static final long serialVersionUID = 1L;

    private String postalCode;
    private String locality;

    public Place(String postalCode, String locality) {
        this.postalCode = postalCode;
        this.locality = locality;
    }

    public String getPostalCode() { return postalCode; }
    public String getLocality() { return locality; }
}
