package tcp01;

import java.io.Serializable;

public class Person implements Serializable {        // permite converter o objeto em bytes
    private static final long serialVersionUID = 1L;  // versão da classe: tem de ser igual nos dois lados

    private String name;
    private Place place;                              // dependência: é enviada automaticamente com a Person
    private int year;

    public Person(String name, Place place, int year) {
        this.name = name;
        this.place = place;
        this.year = year;
    }

    public String getName() { return name; }
    public Place getPlace() { return place; }
    public int getYear() { return year; }
}
