from bs4 import BeautifulSoup
import requests
import sqlite3
import json

import networkx as nx
import matplotlib.pyplot as plt

class Parse:
    def __init__(self):
        pass

    def getInfoPerson(self, URL):
        person_amplua_list=[]
        response = requests.get(URL)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'lxml')

            person_info_div = soup.find('div', class_='person_info')
            if person_info_div.findChild('div', class_='person-genres') != None:
                amplua_list = person_info_div.findChild('div', class_='person-genres').findChild('ul').findAll('li')
            else:
                amplua_list = ''

            person_id_href = soup.findChild('link', {'rel':'canonical'}).attrs['href']
            person_id = ''
            for elem in person_id_href:
                if elem.isdigit():
                    person_id += elem
            person_name = person_info_div.findChild('h1', class_='person-page__title').findChild('div', class_='person-page__title-elements').text.strip(' ')
            print(person_info_div)
            if person_info_div.findChild('meta', {'itemprop':'birthDate'}) != None:
                person_birth_date = person_info_div.findChild('meta', {'itemprop':'birthDate'}).attrs['content']
            else:
                person_birth_date = ''
            person_sex = person_info_div.findChild('meta', {'itemprop':'gender'}).attrs['content']
            for elem in amplua_list:
                person_amplua_list.append(elem.findChild('span', {'itemprop':'jobTitle'}).text)
        else:
            print('theres a problem while getting a response')
        return person_id, person_name, person_birth_date, person_sex, person_amplua_list

    def getInfoMovie(self, URL):
        response = requests.get(URL)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'lxml')

            movie_id_href = soup.find('link', {'rel':'canonical'}).attrs['href']
            movie_id = ''
            for elem in movie_id_href:
                if elem.isdigit():
                    movie_id += elem
            if soup.find('h1', class_='film-page__title-text') != None:
                movie_name = soup.find('h1', class_='film-page__title-text').text
            else:
                movie_name = ''
            movie_genre = list(map(lambda elem: elem.attrs['content'], soup.findAll('li', {'itemprop':'genre'})))
        else:
            print('theres a problem while getting a response')
        return movie_id, movie_name, movie_genre

    def getAllMovies(self, movie_id):
        URL = f'https://rus.kinorium.com/name/{movie_id}/'
        response = requests.get(URL)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'lxml')

            films_groups_list_div = soup.find('div', class_='filmList')
            films_groups_list = films_groups_list_div.findChildren('div', class_='ref-list')

            movies_actor_list = {}
            movies_director_list = {}

            for elem in films_groups_list:
                films_list_key = elem.attrs['data-title'].split()[-1]
                if films_list_key == 'Актёр' or films_list_key == 'Актриса':
                    movies = elem.findChildren('i', class_='movie-title__text')
                    for movie in movies:
                        movie_name = movie.text.replace(u'\xa0', u' ')
                        movie_id = int(movie.attrs['data-id'])
                        movies_actor_list[movie_id] = movie_name
                elif films_list_key == 'Режиссёр':
                    movies = elem.findChildren('i', class_='movie-title__text')
                    for movie in movies:
                        movie_name = movie.text.replace(u'\xa0', u' ')
                        movie_id = int(movie.attrs['data-id'])
                        movies_director_list[movie_id] = movie_name
                        print(movies_director_list)
        else:
            print('theres a problem while getting a response')

        return movies_actor_list, movies_director_list
    def getAllPersons(self, person_id):
        URL = f'https://rus.kinorium.com/{person_id}/cast'
        response = requests.get(URL)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'lxml')

            person_list_div = soup.find('div', class_='personList')

            person_actor_list = {}
            person_director_list = {}

            persons = person_list_div.findChildren('div', {'class': 'cast-page__item', 'itemprop':'actor'})
            for person in persons:
                person_name = person.findChild('h5', {'itemprop': 'name'}).text.replace(u'\xa0', u' ')
                person_id = int(person.findChild('a', class_='cast-page__link-name').attrs['data-id'])
                person_actor_list[person_id] = person_name

            persons = person_list_div.findChildren('div', {'class':'cast-page__item', 'itemprop':'director'})
            for person in persons:
                person_name = person.findChild('h5', {'itemprop': 'name'}).text.replace(u'\xa0', u' ')
                person_id = int(person.findChild('a', class_='cast-page__link-name').attrs['data-id'])
                person_director_list[person_id] = person_name
        else:
            print('theres a problem while getting a response')

        return person_actor_list, person_director_list

    def addPersonDatabase(self, URL, ancestor, kindred = 0):
        person_id, person_name, person_birth_date, person_sex, person_amplua_list = self.getInfoPerson(URL)
        movies_actor_list, movies_director_list = self.getAllMovies(person_id)

        db = sqlite3.connect('server.db')
        sql = db.cursor()

        sql.execute("""CREATE TABLE IF NOT EXISTS persons (
            person_id TEXT,
            person_name TEXT,
            person_amplua TEXT,
            person_birth_date TEXT,
            person_sex TEXT,
            movies_actor TEXT,
            movies_director TEXT,
            ancestor TEXT,
            kindred text
        )""")
        db.commit()

        sql.execute(f"SELECT person_id FROM persons where person_id = '{person_id}'")
        if sql.fetchone() is None:
            sql.execute(f"INSERT INTO persons(person_id, person_name, person_amplua, person_birth_date, person_sex, movies_actor, movies_director, ancestor, kindred) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", (str(person_id), str(person_name), str(person_amplua_list), str(person_birth_date), str(person_sex), json.dumps(movies_actor_list, ensure_ascii=False), json.dumps(movies_director_list, ensure_ascii=False), str(ancestor), str(kindred)))
            db.commit()
        else:
            pass

        return movies_actor_list, movies_director_list

    def addMovieDatabase(self, URL):
        movie_id, movie_name, movie_genre = self.getInfoMovie(URL)
        person_actor_list, person_director_list = self.getAllPersons(movie_id)

        db = sqlite3.connect('server.db')
        sql = db.cursor()

        sql.execute("""CREATE TABLE IF NOT EXISTS movies (
            movie_id TEXT,
            movie_title TEXT,
            movie_genre TEXT,
            persons_actor TEXT,
            persons_director TEXT
        )""")
        db.commit()

        sql.execute(f"SELECT movie_id FROM movies where movie_id = '{movie_id}'")
        if sql.fetchone() is None:
            sql.execute(
                f"INSERT INTO movies(movie_id, movie_title, movie_genre, persons_actor, persons_director) VALUES (?, ?, ?, ?, ?)",
                (str(movie_id), str(movie_name), str(movie_genre),
                 json.dumps(person_actor_list, ensure_ascii=False),
                 json.dumps(person_director_list, ensure_ascii=False)))
            db.commit()
        else:
            pass

        for elem in sql.execute("SELECT * FROM movies"):
            print(elem)

        return person_actor_list, person_director_list

    def personParsingAlternate(self, person_id, ancestor, kindred, max_kindred):
        while kindred < max_kindred:
            URL = f'https://rus.kinorium.com/name/{person_id}/'
            movies_actor_list, movies_director_list = self.addPersonDatabase(URL, ancestor, kindred)
            kindred += 1
            print(kindred)
            for key in movies_actor_list:
                URL1 = f'https://rus.kinorium.com/{key}/'
                person_actor_list1, person_director_list1 = self.addMovieDatabase(URL1)
                for key2 in person_actor_list1:
                    self.personParsingAlternate(key2, person_id, kindred, max_kindred)
                for key2 in person_director_list1:
                    self.personParsingAlternate(key2, person_id, kindred, max_kindred)

            for key in movies_director_list:
                URL1 = f'https://rus.kinorium.com/{key}/'
                person_actor_list1, person_director_list1 = self.addMovieDatabase(URL1)
                for key2 in person_actor_list1:
                    self.personParsingAlternate(key2, person_id, kindred, max_kindred)
                for key2 in person_director_list1:
                    self.personParsingAlternate(key2, person_id, kindred, max_kindred)

    def createNetwork(self, kindred):
        graph = nx.DiGraph()

        db = sqlite3.connect('server.db')
        sql = db.cursor()

        if kindred == 3:
            for node in sql.execute(f"SELECT person_name, person_id FROM persons where kindred = 0"):
                graph.add_node(node)
                ancestor = node[1]
                for node1 in sql.execute(f"SELECT person_name, person_id FROM persons where kindred = 1 and ancestor = '{ancestor}'"):
                        graph.add_node(node1)
                        graph.add_edge(node, node1)
                        ancestor1 = node1[1]
                        for node2 in sql.execute(f"SELECT person_name, person_id FROM persons where kindred = 2 and ancestor = '{ancestor1}'"):
                            graph.add_node(node2)
                            graph.add_edge(node1, node2)
                            ancestor2 = node2[1]
                            for node3 in sql.execute(f"SELECT person_name, person_id FROM persons where kindred = 3 and ancestor = '{ancestor2}'"):
                                graph.add_node(node3)
                                graph.add_edge(node2, node3)

        if kindred == 2:
            for node in sql.execute(f"SELECT person_name, person_id FROM persons where kindred = 0"):
                graph.add_node(node)
                ancestor = node[1]
                for node1 in sql.execute(f"SELECT person_name, person_id FROM persons where kindred = 1 and ancestor = '{ancestor}'"):
                        graph.add_node(node1)
                        graph.add_edge(node, node1)
                        ancestor1 = node1[1]
                        for node2 in sql.execute(f"SELECT person_name, person_id FROM persons where kindred = 2 and ancestor = '{ancestor1}'"):
                            graph.add_node(node2)
                            graph.add_edge(node1, node2)

        if kindred == 1:
            for node in sql.execute(f"SELECT person_name, person_id FROM persons where kindred = 0"):
                graph.add_node(node)
                ancestor = node[1]
                for node1 in sql.execute(f"SELECT person_name, person_id FROM persons where kindred = 1 and ancestor = '{ancestor}'"):
                        graph.add_node(node1)
                        graph.add_edge(node, node1)

        pos = nx.spring_layout(graph)

        nx.draw_networkx_nodes(graph, pos)
        nx.draw_networkx_labels(graph, pos)
        nx.draw_networkx_edges(graph, pos, edge_color='r')

        plt.show()

def main():
    d = Parse()
    print('Welcome to the "godDamnNets" apllication !!! \n'
          'Please choose your option:\n'
          '1. Add person to database \n'
          '2. Create net')
    tick = input('Your option:')
    if tick == '1':
        print('In order to add person to the database you need to enter his/her id and max degree of kindred with next parseable persons: \n')
        person_id = input('Enter person id: ')
        max_kindred = input('Enter degree of kindred: ')
        d.personParsingAlternate(person_id, person_id, 0, max_kindred)
        print('Person was succesfully added to the database')
    elif tick == '2':
        print(
            'In order to add person to the database you need to enter max degree of kindred with next parseable persons: \n'
            '!!! IN CURRENT VERSION OF PROGRAMME MAX DEGREE OF KINDRED CANT BE MORE THAN 3')
        max_kindred = input('Enter degree of kindred: ')
        d.createNetwork(max_kindred)
        print('HERE`S YOUR NET !!!')
    else:
        print('try again')
        return

main()