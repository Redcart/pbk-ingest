from datetime import datetime
import pytz

now = datetime.now(pytz.timezone("UTC"))
print(now)  # Output: Current date and time in UTC
var = now.strftime("%Y-%m-%d %H:%M:%S")
datetime_object = datetime.strptime(var, "%Y-%m-%d %H:%M:%S")
print(datetime_object)  # Output: 2025-06-15 10:00:00
current_ymd = datetime_object.strftime("%Y-%m-%d")
print(current_ymd)  # Output: 2025-06-15