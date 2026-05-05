The schema for the JailReport entity is as follows:

{
  "name": "JailReport",
  "type": "object",
  "properties": {
    "report_date": {
      "type": "string",
      "description": "Report published date (e.g. 5/4/2026)"
    },
    "arrests_date": {
      "type": "string",
      "description": "Booking period date (e.g. 5/3/2026)"
    },
    "total_bookings": {
      "type": "number",
      "description": "Total number of bookings"
    },
    "top_charge": {
      "type": "string",
      "description": "The most common charge today"
    },
    "charge_mix": {
      "type": "array",
      "description": "Array of charge categories with percentages",
      "items": {
        "type": "object",
        "properties": {
          "label": {
            "type": "string"
          },
          "pct": {
            "type": "number"
          },
          "count": {
            "type": "number"
          }
        }
      }
    },
    "cities": {
      "type": "array",
      "description": "Arrests by city",
      "items": {
        "type": "object",
        "properties": {
          "city": {
            "type": "string"
          },
          "pct": {
            "type": "number"
          },
          "count": {
            "type": "number"
          }
        }
      }
    },
    "bookings": {
      "type": "array",
      "description": "Full booking list",
      "items": {
        "type": "object",
        "properties": {
          "num": {
            "type": "number"
          },
          "name": {
            "type": "string"
          },
          "date": {
            "type": "string"
          },
          "charges": {
            "type": "string"
          },
          "city": {
            "type": "string"
          }
        }
      }
    },
    "is_active": {
      "type": "boolean",
      "description": "Whether this is the currently displayed report",
      "default": true
    }
  },
  "required": [
    "report_date",
    "arrests_date",
    "total_bookings"
  ],
  "rls": {
    "create": {
      "user_condition": {
        "role": "admin"
      }
    },
    "read": {
      "$or": [
        {
          "data.is_active": true
        },
        {
          "user_condition": {
            "role": "admin"
          }
        }
      ]
    },
    "update": {
      "user_condition": {
        "role": "admin"
      }
    },
    "delete": {
      "user_condition": {
        "role": "admin"
      }
    }
  }
}
