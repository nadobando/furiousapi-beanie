from typing import Dict

from furiousapi.db.fields import SortingDirection

from beanie.odm.enums import SortDirection

SORTING_DIRECTION_MAPPING: Dict[SortingDirection, SortDirection] = {
    SortingDirection.ASCENDING: SortDirection.ASCENDING,
    SortingDirection.DESCENDING: SortDirection.DESCENDING,
}
