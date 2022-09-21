
# TODO need to set up a trigger after 2pm

- id: update_morning_commute_sensor
  alias: "Commute - Update morning commute sensor"
  initial_state: "on"
  trigger:
    - platform: time_pattern
      minutes: "/2"
  condition:
    - condition: time
      after: "08:00:00"
      before: "11:00:00"
    - condition: time
      weekday:
        - mon
        - tue
        - wed
        - thu
        - fri
  action:
    - service: homeassistant.update_entity
      target:
        entity_id: sensor.morning_commute

# Test

```
source venv/bin/activate
python3 test.py
```

# Registering for ENTSO-E Transparency API token

```
To request access to the Restful API, please register on the Transparency Platform and send an email to transparency@entsoe.eu with “Restful API access” in the subject line. Indicate the email address you entered during registration in the email body. The ENTSO-E Helpdesk will make their best efforts to respond to your request within 3 working days.
```

https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide.html#_authentication_and_authorisation
