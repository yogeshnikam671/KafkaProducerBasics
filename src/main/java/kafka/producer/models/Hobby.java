package kafka.producer.models;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Hobby {
    @JsonProperty("hobbyName")
    private String hobbyName;
    @JsonProperty("reason")
    private String reason;

    public Hobby() {
    }

    public String getHobbyName() {
        return hobbyName;
    }

    public void setHobbyName(String hobbyName) {
        this.hobbyName = hobbyName;
    }

    public String getReason() {
        return reason;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }

    @Override
    public String toString() {
        return "Hobby{" +
                "hobbyName='" + hobbyName + '\'' +
                ", reason='" + reason + '\'' +
                '}';
    }
}
