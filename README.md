# Flink CEP Demo

## Description

1. source: a Event stream, the event with a random int value between [0, 10) is created per 10ms.

2. warning: a Event stream, filtered from source with a condition which the event's value is increasing in 5s, and the greater value event is filtered out.

3. alert: a Event stream, filtered from warning with a condition which the event's value is increasing in 15s, and the greater value event is filtered out.