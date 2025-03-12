from datetime import (
    datetime,
    timezone,
)
from typing import Optional

from prefect import flow


@flow
def what_day_is_it(date: Optional[datetime] = None):
    if date is None:
        date = datetime.now(timezone.utc)
    print(f"It was {date.strftime('%A')} on {date.isoformat()}")


if __name__ == "__main__":
    what_day_is_it("2021-01-01T02:00:19.180906")