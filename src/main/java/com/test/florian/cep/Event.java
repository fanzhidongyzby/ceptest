package com.test.florian.cep;

import org.apache.hadoop.util.Time;

import java.util.Random;

/**
 * Created by zhidong.fzd on 17/3/2.
 */
public class Event {

    private static int MAX_VALUE = 10;

    private static Random random = new Random(Time.now());


    private int value;

    private Event() {}

    public static Event createEvent() {
        Event event = new Event();

        event.value = (int) (random.nextDouble() * MAX_VALUE);

        return event;
    }

    public int getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "Event{" +
                "value=" + value +
                '}';
    }
}
