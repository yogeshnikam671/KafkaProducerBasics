package kafka.producer.models;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Arrays;

public class People {
    @JsonProperty("name")
    private String name;
    @JsonProperty("age")
    private int age;
    @JsonProperty("hobbies")
    private Hobby[] hobbies;

    public People() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    @Override
    public String toString() {
        return "People{" +
                "name='" + name + '\'' +
                ", age=" + age +
                ", hobbies=" + Arrays.toString(hobbies) +
                '}';
    }

    public void setAge(int age) {
        this.age = age;
    }

    public Hobby[] getHobbies() {
        return hobbies;
    }

    public void setHobbies(Hobby[] hobbies) {
        this.hobbies = hobbies;
    }
}
