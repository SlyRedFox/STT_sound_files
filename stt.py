import os
import os.path
from sys import exit
# для запроса через ffmpeg
import subprocess
# для PUT-запроса на "длинное", urllin.request и requests - не работают
# по совету Яндекса: https://cloud.yandex.ru/docs/storage/instruments/boto
import boto3
import requests
# для обработки и конвертации mp3 в ogg (не делает кодек opus, только vorbis)
from pydub import AudioSegment

import logging
from datetime import datetime
from time import sleep
import time
from pprint import pprint
# для реализации класса данных:
# https://www.python.org/dev/peps/pep-0557/
# https://habr.com/ru/post/415829/
# https://docs.python.org/3/library/dataclasses.html
# с версией pyling 2.2.2 после импорта всех четырёх модулей была ошибка:
# Instance of 'Field' has no 'append' member
# при попытке вызвать методы append списка и items словаря
# обновил pylint до 2.4.2 версии: pip install --upgrade pylint
from dataclasses import dataclass
from dataclasses import field
from typing import List
from typing import Dict


@dataclass
class SpeechToText:
    """Класс предназначен для распознавания аудиофайлов (mp3) и последюущим
    сохранением распознанного текста в txt-файлах."""

    now_time: float = time.time()

    # Используем класс данных вместо __init__:
    mp3_catalog: str = 'C:\\MVideo_mp3'
    ogg_catalog: str = 'C:\\MVideo_ogg'
    txt_catalog: str = 'C:\\MVideo_txt'
    log_catalog: str = 'C:\\MVideo_logs'

    # количество mp3-файлов в папке (mp3_catalog)
    count_mp3_files: int = 0
    # количество mp3-файлов в папке (ogg_catalog)
    count_ogg_files: int = 0

    # список mp3-файлов
    mp3_temporary_list: List[str] = field(default_factory=list)
    # список переконвертированных ogg-файлов
    ogg_temporary_list: List[str] = field(default_factory=list)

    # переменная для самого длинного ogg-файла
    longest_ogg: int = 0

    # ключ API (использовать пакет python-dotenv в перспективе)
    __api_token: str = 'secret_deleted_token'

    # для счётчика номеров отображаемых при обработке "коротких" файлов
    count_schetchik: int = 0

    # списки для "коротких" и "длинных" ogg-файлов в start_processing()
    short_ogg_list: List[str] = field(default_factory=list)
    long_ogg_list: List[str] = field(default_factory=list)

    # списки из метода load_to_backet()
    # cписок словарей объектов в бакете - для их последующего удаления
    final_deleting_list: List[dict] = field(default_factory=list)
    # список содержимого бакета вида 'путь/имя_файла' для long-распознавания
    final_way_of_files: List[str] = field(default_factory=list)

    # словарь {'имя файла': 'id'} - хранит id всех ogg-файлов бакета в send_to_long_recognition()
    all_files_id: Dict[str, str] = field(default_factory=dict)


    def start_processing(self) -> None:
        """Конвертируем mp3 в ogg, создаём списки "коротких"/"длинных" файлов"""
        # проверяем наличие рабочих каталогов
        print('Каталог для mp3-файлов существует!') if os.path.exists(self.mp3_catalog) else os.mkdir(self.mp3_catalog)
        print('Каталог для ogg-файлов существует!') if os.path.exists(self.ogg_catalog) else os.mkdir(self.ogg_catalog)
        print('Каталог для txt-файлов существует!') if os.path.exists(self.txt_catalog) else os.mkdir(self.txt_catalog)
        print('Каталог для логов приложения существует!') if os.path.exists(self.log_catalog) else os.mkdir(self.log_catalog)

        # генерируем список всех файлов в папке с mp3 (mp3_catalog)
        self.mp3_temporary_list = [mp3_file for mp3_file in os.listdir(self.mp3_catalog)]

        # если mp3-файлов нет - сообщаем оба этом и выходим из программы
        if self.mp3_temporary_list == []:
            print('\nВнимание!')
            print('Похоже, в папке ' + self.mp3_catalog + ' нет mp3-файлов!')
            input('Нажмите "Enter" для выхода из программы...')
            exit(1)

        # Перекодируем файлы mp3 ---> ogg через ffmpeg
        print('\nНачинаем конвертацию файлов mp3 ---> ogg:')

        for mp3_file in self.mp3_temporary_list:
            # параметры ffmpeg: -y перезапись файлов, -b - битрейт, -с - нужный кодек
            # полное описание: https://ffmpeg.org/ffmpeg.html
            subprocess.call(['ffmpeg','-i', self.mp3_catalog + '\\' + mp3_file, '-y', '-b:a', '96k', '-c:a', 'libopus',
                            self.ogg_catalog + '\\' + mp3_file[:-3] + 'ogg'],
                            shell=True,
                            stdout=subprocess.PIPE)

            print('File ' + mp3_file + ' - переконвертирован в ogg-разрешение!\n\n')

        print('\nКонвертация всех файлов в формат ogg завершена!')

        # узнаём количество файлов mp3 и ogg
        self.count_mp3_files = len(os.listdir(self.mp3_catalog))
        self.count_ogg_files = len(os.listdir(self.ogg_catalog))
        print('\nКоличество mp3-файлов: ' + str(self.count_mp3_files), end='\n')
        print('Количество ogg-файлов: ' + str(self.count_ogg_files), end='\n')

        # список переконвертированных ogg-файлов
        self.ogg_temporary_list = [ogg_file for ogg_file in os.listdir(self.ogg_catalog)]

        # Ищем длительность файла (метод duration_seconds модуля pydub)
        for ogg_file in self.ogg_temporary_list:
            one_ogg_file = AudioSegment.from_ogg(self.ogg_catalog + '\\' + ogg_file)

            # + счётчик номера отображаемых файлов
            self.count_schetchik += 1

            print()
            print('№', self.count_schetchik, 'из', str(self.count_mp3_files))
            print('Обрабатывается файл: '+ ogg_file, '\nДлительность (секунд): ' + str(int(one_ogg_file.duration_seconds)))

            # сохраняем значение самого длительного файла для таймера распознавания
            if int(one_ogg_file.duration_seconds) > self.longest_ogg:
                self.longest_ogg = one_ogg_file.duration_seconds

            # в зависимости от длительности файла добавляем его в один из списков
            if int(one_ogg_file.duration_seconds) <= 20:
                self.short_ogg_list.append(ogg_file)
            else:
                self.long_ogg_list.append(ogg_file)

        # Итоговые списки "коротких" и  "длинных" файлов
        print('\n"Короткие" файлы:', self.short_ogg_list)
        print('"Длинные" файлы:', self.long_ogg_list)

        # Начинаем "короткое" распознавание
        self.short_recognition(self.short_ogg_list)


    def save_in_log(self) -> 'saving logs':
        """Создание лог-файла при вызове исключения"""
        logging.basicConfig(
                            filename=self.log_catalog + '\\' + datetime.today().strftime('%d_%B_%Y') + '.log',
                            format='%(levelname)s [%(asctime)s] %(funcName)s: %(message)s',
                            level=logging.DEBUG
                            )


    def txt_recording(self, recognized_info: str, file_name: str) -> 'txt-files':
        """Сохраняем данные в txt-файлах"""
        print('Записываем данные в txt-файл, путь: '+ self.txt_catalog)
                # file_name[:-3] - отсекаем расширение ogg в имени файла
        try:
            with open(self.txt_catalog + '\\' + file_name[:-3] + 'txt', 'a', encoding='utf-8') as recog_file:
                print(recognized_info, file=recog_file)
                print('Расшифровка записи сохранена в txt-файле: ' + file_name[:-3] + 'txt')
        except Exception as err:
            print('Сохранение txt-данных файла ' + file_name + ' выполнить не удалось! Детали: ', str(err))
            self.save_in_log()
            logging.error('Сохранение txt-данных файла ' + file_name + ' выполнить не удалось! \nDetails: ' + str(err))


    def short_recognition(self, short_files_list: list) -> 'txt-files':
        """Распознаём 'короткие' файлы, сохраняем даныне в txt-файлы"""
        print('\nРаспознаём "короткие" файлы длительностью <= 20 секунд.')

        # параметры запроса и заголовки
        params_for_zapros = '&'.join(['topic=general', 'lang=ru-RU'])
        headers = {'Authorization': 'Api-Key ' + self.__api_token, 'Cache-Control': 'no-cache'}

        for one_short in short_files_list:
            print(f'\nНаправляем ogg-файл {one_short} на "короткое" распознавание...')

            # читаем файл как бинарник для передачи в поле data при "short" распознавании
            # для "long" распознавания rb не обязателен, работаем с файлами из бакета
            with open(self.ogg_catalog + '\\' + one_short, 'rb') as some_file:
                binary_ogg = some_file.read()

            try:
                info_obj = requests.post('https://stt.api.cloud.yandex.net/speech/v1/stt:recognize?' + params_for_zapros, data=binary_ogg, headers=headers)
            except Exception as err:
                print('Ошибка при "коротком" распознавании файла ' + one_short + ' детали:', str(err))
                self.save_in_log()
                logging.error('Ошибка при "коротком" распознавании файла: ' + one_short + '\nDetails: ' + str(err))

            # info_obj - словарь вида {'result': 'str'}
            # извлекаем результат распознавания я записываем в txt-файл
            final_short_recognize = info_obj.json()['result']

            # вызываем метод для записи результатов распознавания в txt-файл
            self.txt_recording(final_short_recognize, one_short)

        print('\nРаспознавание "коротких" файлов завершено!\n\n')

        # стартуем загрузку ogg-файлов в бакет
        self.load_to_backet(self.long_ogg_list)


    def load_to_backet(self, long_file_list: list) -> None:
        """Загрузка "длинных" файлов в бакет, создание списков (на распознавание и удаление)."""

        # инициируем сессию boto3:
        # https://cloud.yandex.ru/docs/storage/instruments/boto
        session = boto3.session.Session()
        s3 = session.client(service_name='s3', endpoint_url='https://storage.yandexcloud.net')

        print('\nЗагружаем данные в бакет.')
        for one_long in self.long_ogg_list:

            print('\nЗагружаем"длинный" ogg-файл в бакет: ' + one_long)
            try:
                s3.upload_file(self.ogg_catalog + '\\' + one_long, 'yandex-backet-for-stt', 'Sound_files/' + one_long)
                print('Загрузка файла выполнена:', one_long)
            except Exception as error_loading:
                print('При загрузке файла: ' + one_long + ' возникла ошибка:', str(error_loading))
                self.save_in_log()
                logging.error('При загрузке файла: ' + one_long + ' возникла ошибка:' + str(error_loading))

        # Получаем все данные по объектам в бакете
        print('\nВсе файлы, которые хранятся сейчас в бакете:')
        print(s3.list_objects(Bucket='yandex-backet-for-stt')['Contents'])

        for file_name in s3.list_objects(Bucket='yandex-backet-for-stt')['Contents']:
            # Cписок путей-ссылок на файлы в бакете - для распознавания записей
            self.final_way_of_files.append(file_name['Key'])

            # Cписок словарей объектов в бакете - для последующего удаления
            temp_dict_from_backet = {'Key': file_name['Key']}
            self.final_deleting_list.append(temp_dict_from_backet)
            temp_dict_from_backet = {}

        # del нулевой индекс в списках - это папка Sound_files в бакете
        self.final_deleting_list = self.final_deleting_list[1:]
        self.final_way_of_files = self.final_way_of_files[1:]

        # отправляем данные на "длинное" распознавание, получаем id операции
        self.send_to_long_recognition()


    def send_to_long_recognition(self):
        """Отправка на "длинное" распознавание, получение id операций."""
        print('Отправляем файлы на "длинное" распознавание.')

        post_recognize_ssilka = 'https://transcribe.api.cloud.yandex.net/speech/stt/v2/longRunningRecognize'

        for way in self.final_way_of_files:
            print('Список файлов в начале выполнения цикла:')
            pprint(self.final_way_of_files)

            ssilka_na_file = 'https://storage.yandexcloud.net/yandex-backet-for-stt/' + way

            # По инструкции https://cloud.yandex.ru/docs/speechkit/stt/transcribation#otpravit-fajl-na-raspoznavanie
            # есть параметры в теле запроса. По примеру:
            # https://stackoverflow.com/questions/9733638/post-json-using-python-requests
            # формируем param_post_ - словари, где значением является тоже словарь
            # params_post_1 = {'specification':{'languageCode': 'ru-RU', 'profanityFilter': 'false', 'audioEncoding': 'OGG_OPUS'}}
            params_post_1 = {'specification':{'languageCode': 'ru-RU'}}
            params_post_2 = {'uri': ssilka_na_file}
            headers = {'Authorization': 'Api-Key ' + self.__api_token, 'Cache-Control': 'no-cache', 'Content-Type': 'application/json'}

            try:
                info_obj = requests.post(post_recognize_ssilka, json={'config': params_post_1, 'audio': params_post_2}, headers=headers)
            except Exception as err:
                print('Ошибка обращения при POST-запроса на распознавание ogg-файла:', str(err))
                self.save_in_log()
                logging.error('При загрузке файла: ' + way + ' возникла ошибка: ' + str(err))

            print('\n\nНа распознавание был отправлен файл', way)
            print('Получение id файла, отправленного на распознавание.')

            # sic!
            # при получении данных файла, отправленного на распознавание, иногда получаем:
            # {'code': 13, 'message': 'Internal'} - ответ от облака Яндекса
            # детали: https://cloud.yandex.ru/docs/api-design-guide/concepts/errors
            # Проверяем на наличие ключа code, при желании, можно добавить другие коды ошибок
            key_error = 'code'

            if key_error in info_obj.json():
                print('При получении id файла ' + way + ' возникло исключение! Код: ' + str(info_obj.json()['code']))

                if info_obj.json()['code'] == 13:
                    print('Internal (500) ошибка сервера Яндекса. Нехватка вычислительных ресурсов.')

                print('\nДобавляем файл ' + way + ' в конец списка на распознавание.')
                self.final_way_of_files.append(way)

                print('Обновлённый список файлов, которые будут отправлены на распознавание.')
                pprint(self.final_way_of_files)
                continue

            print('Добавление имени файла и id в финальный словарь.\n')
            # way[12:] - убираем Sound_files/ из пути к файлу, получаем чистое имя
            # .json() создаёт из info_obj dict, извлекаем данные по ключу ['id'], финал - str
            self.all_files_id.update({way[12:]: info_obj.json()['id']})

        # используем pprint, чтобы было красиво - и читаемо
        print('\nСохранены id всех файлов, которые над данный момент в бакете.')
        # TODO: del pprint + проверить, что ещё использует и убрать import pprint
        pprint(self.all_files_id)

        # запрашиваем результат распознавания файлов
        self.get_result_of_long_recognition()


    def get_result_of_long_recognition(self) -> 'txt-files':
        """'Спим', после запрашиваем результат распознавания файлов"""
        # вызов sleep-таймера
        # формула расчёта длительности распознавания: 1 мин. файла = 10 сек.
        aprox_recognition_time = ((self.longest_ogg + 120) / 60) * 10
        # TODO: добавить таймер сюда через time.time() - начало распознавания.
        print('\n\nНачинаем распознавание файлов. Оно займёт приблизительно (секунд):', int(aprox_recognition_time))
        sleep(aprox_recognition_time)
        # TODO: узкое место... возможно, следует проверять параметр true при получении результата

        # получаем результат "долгого" распознавания
        print('\n\n Распознавание завершено!')
        print('Получаем результаты распознавания.')

        headers_get = {'Authorization': 'Api-Key ' + self.__api_token, 'Cache-Control': 'no-cache'}

        for key, value in self.all_files_id.items():
            try:
                info_obj_get = requests.get('https://operation.api.cloud.yandex.net/operations/' + value, headers=headers_get)
            except Exception as err:
                print('Ошибка при GET-запросе на получения результатов распознавания "длинного" файла' + key + 'Details:', str(err))
                self.save_in_log()
                logging.error('Ошибка при GET-запросе на получения результатов распознавания файла' + key + 'Details:' + str(err))

            dict_info = info_obj_get.json()['response']

            final_recognition = ''
            for one_elem in dict_info['chunks']:
                final_recognition += '\n' + one_elem['alternatives'][0]['text']
            # запись данных расшифровки в *.txt файл
            self.txt_recording(final_recognition, key)

        print('\nРаспознавание завершено! Текстовые файлы сохранены по пути:', self.txt_catalog)

        # вызываем команду зачистки: из бакета, из папок с файлами mp3 и ogg
        self.extermination_files()

    def extermination_files(self):
        """Удаляем файлы из бакета, а также аудио из папок c mp3 и ogg"""
        print('\nУдаляем все файлы из бакета:')
        pprint(self.final_deleting_list)

        # инициируем сессию boto3... повторно :
        # https://cloud.yandex.ru/docs/storage/instruments/boto
        session = boto3.session.Session()
        s3 = session.client(service_name='s3', endpoint_url='https://storage.yandexcloud.net')

        try:
            s3.delete_objects(Bucket='yandex-backet-for-stt', Delete={'Objects': self.final_deleting_list})
            print('Все файлы удалены из бакета!')
        except Exception as error:
            print('При удалении файлов из бакета произошёл сбой, детали:', str(error))
            self.save_in_log()
            logging.error('При удалении файлов из бакета произошёл сбой, детали: ' + str(error))

        # Удаляем файлы mp3 и ogg из папок
        print('\nУдаляем из рабочих папок файлы mp3 и ogg:')
        for mp3_file in os.listdir(self.mp3_catalog):
            os.remove(self.mp3_catalog + '\\' + mp3_file)
        print('Все mp3-файлы из рабочей папки удалены!')

        for ogg_file in os.listdir(self.ogg_catalog):
            os.remove(self.ogg_catalog + '\\' + ogg_file)
        print('Все ogg-файлы из рабочей папки удалены!')


# Запуск
begin_working = SpeechToText()
begin_working.start_processing()
print('\nРабота программы завершена!')
print('Время выполнения (секунды):', time.time() - begin_working.now_time)
