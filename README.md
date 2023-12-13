# insights-api-db

insights-api-db is a service that manages data processing tasks initiated by Insights API. It reads the tasks from the queue stored in DB and executes these tasks in parallel. Tasks are SQL queries performed on Postgresql server.

Tasks can be of type:
* quality calculation
* stops calculation
* bivariate analytics calculation
* correlation calculation
* overriding bivariate axis labels and stops
* calculating geometry for stat_h3_geom
* deleting outdated indicators

## Database
insights-api-db has no its own database. It uses Insights API database: test, dev and prod instances correspondingly

## Directory and files structure

TBD
