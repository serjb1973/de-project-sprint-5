# Проект 5-го спринта

### Описание
Репозиторий предназначен для сдачи проекта 5-го спринта

### Как работать с репозиторием
1. В вашем GitHub-аккаунте автоматически создастся репозиторий `de-project-sprint-5` после того, как вы привяжете свой GitHub-аккаунт на Платформе.
2. Скопируйте репозиторий на свой локальный компьютер, в качестве пароля укажите ваш `Access Token` (получить нужно на странице [Personal Access Tokens](https://github.com/settings/tokens)):
	* `git clone https://github.com/{{ username }}/de-project-sprint-5.git`
3. Перейдите в директорию с проектом: 
	* `cd de-project-sprint-5`
4. Выполните проект и сохраните получившийся код в локальном репозитории:
	* `git add .`
	* `git commit -m 'my best commit'`
5. Обновите репозиторий в вашем GutHub-аккаунте:
	* `git push origin main`

### Структура репозитория
- `/src/dags`

### Как запустить контейнер
Запустите локально команду:

```
docker run \
-d \
-p 3000:3000 \
-p 3002:3002 \
-p 15432:5432 \
cr.yandex/crp1r8pht0n0gl25aug1/de-pg-cr-af:latest
```

После того как запустится контейнер, вам будут доступны:
- Airflow
	- `localhost:3000/airflow`
- БД
	- `jovyan:jovyan@localhost:15432/de`

### Созданы несколько групп DAG
#### 1. Создание схем и таблиц в БД postgres:
* sprint5_stg_init_schema
* sprint5_dds_init_schema
* sprint5_cdm_init_schema
#### 2. Заполнение stage уровня - схема stg:
* sprint5_stg_from_rest
* sprint5_stg_from_postgres
* sprint5_stg_from_mongo
##### В некоторых вышеуказанных трёх DAG указан лимит на выгрузку объектов на один сеанс, первоначально, для полноты загрузки (на предоставленных тестовых данных) достаточно запустить их три раза.
#### 3. Заполнение dds уровня - схема dds:
* sprint5_dds_dm
* sprint5_dds_fact
##### Загрузка таблиц фактов и измерений разделена согласно заданию спринта
#### 4. Заполнение datamart уровня - схема cdm:
* sprint5_cdm
