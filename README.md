# Finding the most popular user's route through pages of the website

## Description
Given the dataset of all clicks users make on the web site I need to find the most popluar route. 
Firstly I have filtered the dataset so that there would be only events when user changes their page. 
Then I have groupped the dataset by session id and user id, sorted this data by timestamp and for each combination of user id and session id made a list of pages a user has visited.
And then I have found the most frequrntly occuring ones out of them 

## Описание
Проект по оптимизации сайта с поиском самого популярного у пользователей маршрута страниц из базы данных всех кликов пользователей на сайте. 
Дан набор данных clickstream, который содержит клики всех пользователей на сайте. На основе этих данных мне надо найти самый популярный маршрут по страницам среди пользователей 
(то есть, например: Главная страница -> Новости -> Тарифы -> Контакты)
Здесь я использовал python и библиотеку spark для анализа большого набора данных и поиска самого популярного маршрута. 
Сначала я отфильтровал данные чтобы там были только события, когда пользователь переходит на новую страницу. 
Затем я сгруппировал по user и id session id и отсортировал по timestamp и в итоге получил для каждой пары user id и session id
(то есть для каждого уникального визита одного пользователя) список страниц, которые он посетил (event_page) и среди них я нашел наиболее часто встречающиеся 
