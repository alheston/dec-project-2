# **Project Team 6 DEC**
_Josh B_ - _Shruti S_ - _Alex H_

## Objective:
The objective is to provide gold level production tables that can be used in downstream BI tools. 
## Consumers:
The target consumers are daily commuters who are looking to identify the most efficient path to commute from their preferred source location to NY Penn Station. In reality they would access this data via app from either the android or apple os. 

## Questions We Want To Answer:
1) Given your preferred source location, what is the average transit time to Penn Station for typical commuting hours (6-9) historically.
2) Given your preferred source location, what is the average transit time to Penn Station for typical commuting hours (6-9) today.
3) What source locations (of the ones provided) and modes are the most efficient in travel time to Penn Station.

| `Source Name`  | `Source Type` | `Source Docs`                               | `Endpoint` |
| -------------  | ------------- | ------------                                | -----------|
|  northwind   | postgres db     | [https://docs.traveltime.com/api/sdks/python](https://github.com/pthom/northwind_psql?tab=readme-ov-file) | https://docs.traveltime.com/api/reference/travel-time-distance-matrix|


## Architecture:
![image](https://github.com/alheston/dec-project-1/assets/167915392/aefa75a0-9ef4-496f-a5c9-04b9838c93e5)



