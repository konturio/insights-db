# insights-db

insights-db (or insights-api-db-svc) is a service that does heavy statistics calculations for indicators uploaded to Insights API. It creates task queue and executes these tasks in parallel. Tasks are SQL queries performed on Postgresql server.

Tasks can be of type:
* quality calculation
* stops calculation
* bivariate analytics calculation
* correlation calculation

Some jobs are run in the infinite loops:
* creating tasks
* executing tasks
* overriding bivariate axis labels and stops with custom values
* changing indicator statuses
* deleting outdated indicators

## Database
insights-api-db has no its own database. It uses Insights API database: test, dev and prod instances correspondingly

## Directory and files structure

```
├── Dockerfile
├── Makefile   # definitions of jobs
├── README.md
├── procedures
│   └── < definitions of stored procedures >
├── scripts
│   └── < various sql/bash scripts >
└── start.sh   # entry point
```

## k8s instance

container is created inside `*-insights-api` pod (dev, test, prod)

```
$ kubectl get pods  -n test-insights-api -o wide |grep insights-api-db-svc

test-insights-api-db-svc-7955d47896-vjcnp   1/1     Running   0             19h   192.168.251.157   hwn03.k8s-01.kontur.io   <none>           <none>
```

## logs

Kibana logs are parsed from stdout/stderr and available [here](https://kontur-elastic-deployment.kb.eastus2.azure.elastic-cloud.com:9243/app/discover#/?_a=(columns:!(log),filters:!(('$state':(store:appState),meta:(alias:!n,disabled:!f,field:kubernetes.labels.app_kubernetes_io%2Finstance,index:b8683180-0124-11ed-ac3a-d5bb0507369a,key:kubernetes.labels.app_kubernetes_io%2Finstance,negate:!f,params:(query:test-insights-api-db-svc),type:phrase),query:(match_phrase:(kubernetes.labels.app_kubernetes_io%2Finstance:test-insights-api-db-svc)))),hideChart:!t,index:b8683180-0124-11ed-ac3a-d5bb0507369a,interval:auto,query:(language:kuery,query:''),sort:!(!('@timestamp',desc)))&_g=(filters:!(),refreshInterval:(pause:!t,value:60000),time:(from:now-1d,to:now)))

Regular text logs are available via kubectl logs:

```
kubectl logs -n test-insights-api test-insights-api-db-svc-7955d47896-vjcnp -f
```

## Check if main processes are running

login to container
```
kubectl exec -it -n test-insights-api test-insights-api-db-svc-7955d47896-vjcnp -- bash
```

check bash processes. at least 4 of them should be present:
* with remove_outated_indicators.sql
* with apply_all_axis_overrides.sql
* with create_quality_stops_analytics_tasks.sql, create_correlation_tasks.sql and update_indicators_state.sql
* with `call dispatch()` - it executes all the tasks

```
# pgrep -a bash

10 /bin/bash -c while true; do psql -1 -f scripts/remove_outated_indicators.sql; sleep 5m; done
12 /bin/bash -c while true; do psql -f scripts/apply_all_axis_overrides.sql; sleep 5m; done
23 /bin/bash -c while true; do psql -f scripts/create_quality_stops_analytics_tasks.sql; psql -1 -c "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ" -f scripts/create_correlation_tasks.sql -f scripts/update_indicators_state.sql; sleep 1m; done
4092 /bin/bash -c while true; do seq `psql -c 'select count(0) from task_queue' -t` | parallel -j 3 -n0 "psql -q -c 'call dispatch()'"; sleep 1; done
```
