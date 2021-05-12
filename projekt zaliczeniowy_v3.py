# -*- coding: utf-8 -*-
"""
Created on Tue Apr 27 22:09:37 2021

@author: krzysztof
"""


from flask import Flask, request, redirect, render_template,redirect, url_for
from werkzeug.utils import secure_filename
import os
import pandas as pd
from pathlib import Path
from hurry.filesize import size
from sqlalchemy import Column, Integer, String, Float, DateTime
from typing import List
from pydantic import BaseModel
import matplotlib.pyplot as plt
from flask import json
from werkzeug.exceptions import HTTPException
from flask_sqlalchemy import SQLAlchemy
import shutil

os.chdir(r'C:\Users\krzys\Desktop\data science\II semestr\python\projekt zaliczeniowy')

app = Flask (__name__)
wd = r"C:\Users\krzys\Desktop\data science\II semestr\python\projekt zaliczeniowy"
app.config['SQLALCHEMY_DATABASE_URI'] = r"sqlite:///C:\Users\krzys\Desktop\data science\II semestr\python\projekt zaliczeniowy\baza_windows.db"
db = SQLAlchemy(app)


class metadane(db.Model):
    __tablename__ = 'metadane'
    id = Column (Integer, primary_key = True)
    file_name = Column (String (80), unique = True,nullable = True)
    file_size = Column (String(120), nullable = True)    
    columns = Column (Integer, nullable = True)
    rows = Column (Integer, nullable = True)
    records_nmbr = Column (Integer, nullable = True)
    def __repr__(self):
        return f'<Nazwa_pliku: {self.file_name},\
            Rozmiar_pliku: {self.numery},\
                Ilosc_kolumn: {self.columns},\
                    Ilosc_wierszy: {self.rows},\
                        Ilosc_rekordow: {self.records_nmbr}>'

class data_db_object(db.Model):
    __tablename__ = 'data_db_object'
    id = Column (Integer, primary_key = True)
    file_name = Column (String (80), nullable = True)
    column_name = Column (String (80),nullable = True)
    dtype = Column (String (10),nullable = True)
    unique_values = Column (Integer, nullable = True)    
    empty_values = Column (Integer, nullable = True)
    nan_values = Column (Integer, nullable = True)    
    def __repr__(self):
        return f'<Nazwa_pliku: {self.file_name},\
            Typ_danych: {self.dtype},\
                Nazwa_kolumny: {self.column_name},\
                    Wartosci_unikalne: {self.unique_values},\
                        Puste_wartosci: {self.empty_values},\
                            Wartosci_NaN: {self.nan_values}'
                                
class data_db_numeric(db.Model):
    __tablename__ = 'data_db_numeric'
    id = Column (Integer, primary_key = True)
    file_name = Column (String (80), nullable = True)
    column_name = Column (String (80),nullable = True)
    dtype = Column (String (10),nullable = True)
    min_value = Column (Float, nullable = True)
    mean_value = Column (Float, nullable = True)
    max_value = Column (Float, nullable = True)
    median_value = Column (Float, nullable = True)
    std_dev_value = Column (Float, nullable = True)
    def __repr__(self):
        return f'<Nazwa_pliku: {self.file_name},\
            Typ_danych: {self.dtype},\
                Nazwa_kolumny: {self.column_name},\
                    Wartosc_MIN: {self.min_value},\
                        Wartosc_srednia:{self:mean_value},\
                            Wartosc_MAX: {self.max_value},\
                                Mediana: {self.median_value},\
                                    Odch_std: {self.std_dev_value}>'

class data_db_datetime(db.Model):
    __tablename__ = 'data_db_datetime'
    id = Column (Integer, primary_key = True)
    file_name = Column (String (80), nullable = True)
    column_name = Column (String (80),nullable = True)
    dtype = Column (String (10),nullable = True)
    min_date = Column (DateTime, nullable = True)
    max_date = Column (DateTime, nullable = True)
    def __repr__(self):
        return f'<Nazwa_pliku: {self.file_name},\
            Typ_danych: {self.dtype},\
                Nazwa_kolumny: {self.column_name},\
                    Min_data: {self.min_date},\
                        Max_data: {self.max_date}>'

@app.errorhandler(HTTPException)
def handle_exception(e):
    """Return JSON instead of HTML for HTTP errors."""
    # start with the correct headers and status code from the error
    response = e.get_response()
    # replace the body with JSON
    response.data = json.dumps({
        "code": e.code,
        "name": e.name,
        "description": e.description,
    })
    response.content_type = "application/json"
    return response

@app.route('/')
def przenies():
    return redirect(url_for('pliki'))

#ladowanie pliku - jesli plik jest na serwerze, to go nie laduje
@app.route('/upload', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        f = request.files['plik']
        dataset = pd.read_csv(f, sep = ';')
        r, c = dataset.shape
        if r > 1000 or c > 20:
            return 'Plik ma za duzo kolumn lub wierszy. Maksymalna ilosc \
                wierszy to 1000, kolumn 20.\
                <p><a href="/upload"> Upload</a> \
                <a href="/pliki"> Pliki</a></p>' 
        else:
            filename = secure_filename(f.filename)
            name_with_dir = os.path.join('wrzucone_pliki', filename)
            try:
                open (name_with_dir, 'r')
                return 'Taki plik juz istnieje.\
                        <p><a href="/upload"> Upload</a></p> \
                        <p><a href="/pliki"> Pliki</a></p>' 
            except:
                f.save(name_with_dir)
                dataset.to_csv(r'wrzucone_pliki/{}'.format(filename), sep = ';')
                path = os.getcwd()+"\wrzucone_pliki"
                pliki_rozmiary = {}   
                liczba_rekordow = pd.read_csv(r'C:\Users\krzys\Desktop\data science\II semestr\python\projekt zaliczeniowy\wrzucone_pliki\{}'.format(filename), sep = ';').size
                rows,columns = pd.read_csv(r'C:\Users\krzys\Desktop\data science\II semestr\python\projekt zaliczeniowy\wrzucone_pliki\{}'.format(filename), sep = ';').shape
                pliki_rozmiary[filename] = {'rozmiar_pliku':size(Path(r'C:\Users\krzys\Desktop\data science\II semestr\python\projekt zaliczeniowy\wrzucone_pliki\{}'.format(filename)).stat().st_size), 'kolumny':columns, 'wiersze':rows,'liczba_rekordow':liczba_rekordow}
                for key in pliki_rozmiary.keys():
                    file_name = key
                    file_size = pliki_rozmiary[key]['rozmiar_pliku']
                    columns = pliki_rozmiary[key]['kolumny']
                    rows = pliki_rozmiary[key]['wiersze']
                    records_nmbr = str(pliki_rozmiary[key]['liczba_rekordow'])
                    dane = metadane(file_name = file_name, file_size = file_size, columns = columns, rows = rows, records_nmbr = records_nmbr)
                    db.session.add(dane)
                    try:
                        db.session.commit()
                    except:
                        pass
                statystyki = {}
                wartosci_puste = {}
                wartosci_licznik = {}
                
                #statystyki dla datatime\
                try:
                    dataset = pd.read_csv('{}'.format(filename), sep=';')
                    dataset['data i godzina dodania ogłoszenia'] = pd.to_datetime\
                        (dataset['data i godzina dodania ogłoszenia'])    
                    min_date = min (dataset['data i godzina dodania ogłoszenia'])
                    max_date = max (dataset['data i godzina dodania ogłoszenia']) 
                except:
                    pass
                liczba_rekordow = dataset.size 
                #statystyki dla kolumn "object"
                object_columns = dataset.select_dtypes(include = 'object')
                typy = object_columns.dtypes
                typy_wszystkie = dataset.dtypes
                for kolumna in object_columns.columns:
                    wartosci_licznik [kolumna] = dataset[kolumna].size 
                describe = object_columns.describe()
                describe['unique':'unique']    
                wartosci_unikatowe = object_columns.nunique(axis = 0)
                for i in object_columns.columns:
                    wartosci_puste[i] = (object_columns[i] == '').sum()
                wartosci_nan = object_columns.isnull().sum(axis = 0)       
                #statystyki dla int i float
                numeric_columns = dataset.select_dtypes(include = ['int64', 'float64'])
                opis = numeric_columns.head().describe()
                minimalna = opis["min":"min"]
                srednia = opis["mean":"mean"]
                maksymalna = opis["max":"max"]
                mediana = numeric_columns.median(axis = 0)
                odchylenie_standardowe = opis["std":"std"]                        
                for kolumna in dataset.select_dtypes(include = 'object'):
                    statystyki = data_db_object(file_name = '{}'.format(filename),\
                                         column_name = kolumna,\
                                         dtype = str(typy[kolumna]),\
                                         unique_values = int (wartosci_unikatowe[kolumna]),\
                                         empty_values = int(wartosci_puste[kolumna]),\
                                         nan_values = int(wartosci_nan[kolumna]))
                    db.session.add(statystyki)
                    db.session.commit()
                for kolumna in dataset.select_dtypes(include = 'datetime'):
                    statystyki = data_db_datetime(file_name = '{}'.format(filename),\
                                         column_name = kolumna,\
                                         dtype = str(typy_wszystkie[kolumna]),\
                                         min_date = min_date,\
                                         max_date = max_date)
                    db.session.add(statystyki)
                    db.session.commit()
                for kolumna in dataset.select_dtypes(include = ['float64', 'int64']):
                    statystyki = data_db_numeric(file_name = '{}'.format(filename),\
                                         column_name = kolumna,\
                                         dtype = str(typy_wszystkie[kolumna]),\
                                         min_value = minimalna[kolumna]['min'],\
                                         mean_value = srednia[kolumna]['mean'],\
                                         max_value = maksymalna[kolumna]['max'],\
                                         median_value = mediana[kolumna],\
                                         std_dev_value = odchylenie_standardowe[kolumna]['std'])
                    db.session.add(statystyki)
                    db.session.commit()
                kolumny_do_histogramu = []
                path = r'C:\Users\krzys\Desktop\data science\II semestr\python\projekt zaliczeniowy\histogramy\{}'.format(filename)
                try:
                    os.mkdir(path)
                except FileExistsError:
                    pass
                for i, j in dataset.dtypes.items():
                    if i == 'id':
                        pass
                    else:
                        try:
                            if j in ('int64', 'float64', 'category', 'Bool'):
                                kolumny_do_histogramu.append(i)
                        except TypeError:
                            pass                
                for i in kolumny_do_histogramu:
                    dataset[i].hist()
                    plt.savefig(r'histogramy/{}/{}_hist.png'.format(filename, i))
                    plt.clf()
                return redirect(f'/saved_file/{filename}')
    elif request.method == 'GET':
        return render_template('upload.html')

# strona wyświetlająca się po wysłaniu pliku
@app.route('/saved_file/<filename>')
def saved_file(filename):
     return redirect(url_for('pliki'))

#strona wyswietlajaca nazwy plikow na serwerze i wczytanych plikow
@app.route('/pliki')
def pliki ():
    try:
        path = os.getcwd()+"\wrzucone_pliki"
        dict_of_files = {}
        for f in os.listdir(path):
            dict_of_files[f] = path + '\{}'.format('') + f
        return render_template("list.html", dict_of_files = dict_of_files)
    except:
        return render_template("list.html", dict_of_files = dict_of_files)
    
@app.route('/delete_data/<string:nazwa_pliku>')
def delete_data(nazwa_pliku):
    for tabela in db.metadata.tables.keys():    
        """
        zrobione jako string i następnie exec ze względu na to, że nie mogłem
        użyć  w tym kawalku 'filter({}.file_name==" nazwy pliku jako argumentu
        w petli
        """
        a = 'db.session.query({})'.format(tabela)
        b = 'filter({}.file_name=="{}").delete()'.format(tabela, nazwa_pliku)
        c = a + '.' + b
        exec(c)
        db.session.commit()
    """
    shutil pozwolił mi usunąć folder z plikami
    """
    try:
        os.remove(r'C:\Users\krzys\Desktop\data science\II semestr\python\projekt zaliczeniowy\wrzucone_pliki\{}'.format(nazwa_pliku))
    except:
        pass 
    try:
        shutil.rmtree(r'C:\Users\krzys\Desktop\data science\II semestr\python\projekt zaliczeniowy\histogramy\{}'.format(nazwa_pliku))   
    except:
        pass
    return 'Dane zostały usunięte \
            <p><a href="/upload"> Upload</a> \
            <a href="/pliki"> Pliki</a></p>'   
            

@app.route('/pliki/<string:nazwa_pliku>')
def wyswietl_dane(nazwa_pliku):
    path = os.getcwd()+"\wrzucone_pliki"
    lista_plikow = []
    for f in os.listdir(path):
        lista_plikow.append(f)
    slownik_query = {}
    for tabela in db.metadata.tables.keys():    
        """
        zrobione jako string i następnie eval ze względu na to, że nie mogłem
        użyć  w tym kawalku 'filter({}.file_name==" nazwy pliku jako argumentu
        w petli
        """
        lista_plikow = []
        for f in os.listdir(path):
            lista_plikow.append(f)
        a = 'db.session.query({})'.format(tabela)
        b = 'filter({}.file_name=="{}")'.format(tabela, nazwa_pliku)
        c = a + '.' + b
        slownik_query[tabela] = eval(c)
    if nazwa_pliku in lista_plikow:
        return render_template("list_data.html", slownik_query = slownik_query)
    else:
        return 'Brak takiego pliku na serwerze.\
                <p>Możesz usunac wpisy w bazie danych i zaladowac plik ponownie</p> \
                <p><a href="/delete_data/{}">Usuń</a></p>'.format(nazwa_pliku)
                    
        
if __name__ == "__main__":
    db.create_all()
    app.run(debug = False, port = 1234)

