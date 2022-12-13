import calendar
import os
import re
import sqlite3
import sys
from configparser import ConfigParser
from datetime import date, timedelta, datetime
from time import sleep

import requests
from bs4 import BeautifulSoup


class Scraper:

    def __init__(self):
        self.headers = None
        self.cursor = None
        self.tele_auth_token = None
        self.tel_group_id = None

    def getEventsCalendarToday(self):
        global event_time_holder

        # specify the url
        url = 'https://www.forexfactory.com/calendar?day=today'

        # query the website and return the html to the variable ‘page’
        page = requests.get(url=url, headers=self.headers, timeout=30)

        data = page.content
        # print(page)
        # parse the html using beautiful soup and store in variable `soup`
        soup = BeautifulSoup(data, 'html.parser')
        # Take out the <div> of name and get its value

        # Find the table containing all the data
        table = soup.find('table', class_='calendar__table')

        # Date of Event
        date_of_events = table.find('tr', class_='calendar__row--new-day').find('span', class_='date')

        # Regualr Expression to find the 'day of week', 'month' and the 'day'
        matchObj = re.search('([a-zA-Z]{3})([a-zA-Z]{3}) ([0-9]{1,2})', date_of_events.text)

        # Assigning the 'day of week', 'month' and 'day'

        day_of_week = matchObj.group(1)
        month = matchObj.group(2)
        month = self.strToIntMonth(month)  # Convert from Str to Int
        # if month in monthsList:
        # 	print(month)
        month = int(
            format(month, "02"))  # Places 0's in front of the month if it is single digit day, for months Jan - Sep
        day = matchObj.group(3)
        day = int(
            format(int(day),
                   "02"))  # Places 0's in front of the day if it is single digit day, for days 1-9 of the month
        year = date.today().year

        event_date = str(day) + "/" + str(month) + "/" + str(year)

        # Event Times
        events = table.find_all('td', class_='calendar__time')

        self.eventProcess(events, url, day_of_week, event_date)

        print('Successfully retrieved all data')
        return True

    def getEventsCalendar(self, start_date, end_date):
        global event_time_holder
        url = 'https://www.forexfactory.com/' + start_date
        print(url)

        # query the website and return the html to the variable ‘page’
        page = requests.get(url=url, headers=self.headers).content
        # print(page)
        # parse the html using beautiful soup and store in variable `soup`
        soup = BeautifulSoup(page, 'lxml')
        # Take out the <div> of name and get its value

        # Find the table containing all the data
        table = soup.find('table', class_='calendar__table')

        # Date of Event
        date_of_events = table.find('tr', class_='calendar__row--new-day').find('span', class_='date')

        # Regualr Expression to find the 'day of week', 'month' and the 'day'
        matchObj = re.search('([a-zA-Z]{3})([a-zA-Z]{3}) ([0-9]{1,2})', date_of_events.text)

        # Assigning the 'day of week', 'month' and 'day'

        day_of_week = matchObj.group(1)
        month = matchObj.group(2)
        month = self.strToIntMonth(month)  # Convert from Str to Int
        # if month in monthsList:
        # 	print(month)
        month = int(
            format(month, "02"))  # Places 0's in front of the month if it is single digit day, for months Jan - Sep
        day = matchObj.group(3)
        day = int(
            format(int(day),
                   "02"))  # Places 0's in front of the day if it is single digit day, for days 1-9 of the month
        year = int(start_date[-4:])

        event_date = str(day) + "/" + str(month) + "/" + str(year)

        # Event Times
        events = table.find_all('td', class_='calendar__time')

        self.eventProcess(events, url, day_of_week, event_date)

        if start_date == end_date:
            print('Successfully retrieved all data')
            return True
        else:
            scrape_next_day = soup.find('div', class_='head').find('a', class_='calendar__pagination--next')[
                'href']
            self.getEventsCalendar(scrape_next_day, end_date)

    def eventProcess(self, events, url, day_of_week, event_date):
        global event_time_holder
        for news in events:
            try:
                status = 'black'
                parent = news.parent
                if 'calendar__row--grey' in str(parent):
                    status = 'grey'

                parentId = parent['data-eventid']
                curr = news.find_next_sibling('td', class_='currency').text.strip()
                impact = news.find_next_sibling('td', class_='impact').find('span')['class']
                impact = impact[0]
                event = news.find_next_sibling('td', class_='event').find('span').text.strip()
                previous = news.find_next_sibling('td', class_='previous').text
                forecast = news.find_next_sibling('td', class_='forecast').text
                actual = news.find_next_sibling('td', class_='actual').text
                event_time = news.text.strip()

                if event_time.upper() == 'ALL DAY' or event_time.upper() == 'TENTATIVE':
                    continue

                if impact == 'high':
                    # if impact != '':
                    if event_time != '':
                        event_time_holder = event_time[:4] + ":00 " + event_time[-2:].upper()
                    sqlite_select_with_param = "SELECT * FROM FFNEWS where event_id = ? and status = ?"
                    self.cursor.execute(sqlite_select_with_param, (parentId + status, status))

                    result = self.cursor.fetchone()
                    if result is None:
                        dateStr = event_date + " " + self.convert24(event_time_holder).strip()
                        datetime_object = datetime.strptime(dateStr, '%d/%m/%Y %H:%M:%S')
                        print("Date send  : " + datetime_object.strftime('%d/%m/%Y %H:%M:%S'))
                        if status == 'black':
                            minutes_diff = (datetime_object - datetime.now()).total_seconds() / 60
                            if minutes_diff < 15:
                                print("send message")
                                self.send_msg(url, parentId, status, day_of_week, dateStr, curr, event, actual,
                                              forecast,
                                              previous, datetime_object, impact)
                        elif status == 'grey':
                            if datetime.now() > datetime_object:
                                print("send message")
                                self.send_msg(url, parentId, status, day_of_week, dateStr, curr, event, actual,
                                              forecast,
                                              previous, datetime_object, impact)
            except Exception as e:
                print("There was an error: " + str(e))
                continue

    def send_msg(self, url, parent_id, status, day_of_week, date_str, curr, event, actual, forecast, previous,
                 datetime_object,
                 impact):
        urlDetail = url + "#detail=" + parent_id
        print(urlDetail)
        # driver = webdriver.Chrome(cwd + '/chromedriver')
        # driver.get(urlDetail)
        # driver.refresh()
        # sleep(2)
        # pageDetailRes = requests.get(url=urlDetail, headers=headers).text
        # pageDetail = BeautifulSoup(pageDetailRes, 'html.parser')
        # print(pageDetail)
        # link_text = pageDetail.find('a', href= True, text='latest release')
        link_text = urlDetail
        # print(link_text)
        # link_text = driver.find_element(By.LINK_TEXT, 'latest release').get_attribute('href')
        sleep(2)
        # driver.quit()
        message = ''

        impact_str = 'News'
        if impact == 'high':
            impact_str = 'High Impact News'
        elif impact == 'medium':
            impact_str = 'Medium Impact News'
        elif impact == 'low':
            impact_str = 'Low Impact News'

        if status == 'grey':
            message = 'Released\n' + impact_str + '\nDate : ' + day_of_week + ' ' + date_str + \
                      '\nCurrency : ' + curr + '\nEvent Title : ' + event + '\nActual : ' + actual + \
                      '\nForecast : ' + forecast + '\nPrevious : ' + previous + '\nMore info : ' + link_text
        elif status == 'black':
            message = 'Will be Released\n' + impact_str + '\nDate : ' + day_of_week + ' ' + date_str + \
                      '\nCurrency : ' + curr + '\nEvent Title : ' + event + '\nActual : ' + actual + \
                      '\nForecast : ' + forecast + '\nPrevious : ' + previous + '\nMore info : ' + link_text

        print(message)
        # if send_msg_on_telegram(message):
        #     sqlite_insert_with_param = """INSERT INTO FFNEWS (event_id, message, publish_date, title, curreny, actual,
        #                                       forecast, previous, link, status)
        #                                                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
        #     data_tuple = (
        #         parent_id + status, message, datetime_object, event, curr, actual, forecast, previous, link_text, status)
        #     conn.execute(sqlite_insert_with_param, data_tuple)
        #     conn.commit()

    def convert24(self, str1):
        # Checking if last two elements of time
        # is AM and first two elements are 12
        if str1[-2:] == "AM" and str1[:2] == "12":
            return "00" + str1[2:-2]

        # remove the AM
        elif str1[-2:] == "AM":
            return str1[:-2]

        # Checking if last two elements of time
        # is PM and first two elements are 12
        elif str1[-2:] == "PM" and str1[:2] == "12":
            return str1[:-2]

        else:
            # add 12 to hours and remove PM
            split = str1.split(":")
            return str(int(split[0]) + 12) + ":" + split[1] + ":" + split[2][:2]

    def strToIntMonth(self, month):
        #
        # Function to convert Str Month into an Int
        #

        if month == 'Jan':
            return 1
        elif month == "Feb":
            return 2
        elif month == "Mar":
            return 3
        elif month == "Apr":
            return 4
        elif month == "May":
            return 5
        elif month == "Jun":
            return 6
        elif month == "Jul":
            return 7
        elif month == "Aug":
            return 8
        elif month == "Sep":
            return 9
        elif month == "Oct":
            return 10
        elif month == "Nov":
            return 11
        elif month == "Dec":
            return 12
        else:
            return None

    def timeDateAdjust(event_time_hour, event_time_minutes, am_or_pm, hours_to_adjust, year, month, day):
        d = date(year, month, day)

        if am_or_pm == "am":
            adjusted_hour = int(
                event_time_hour) + hours_to_adjust
            # Hours_to_adjust variable is used to adjust for timezone differences as the forex factory calendar is in EST
        else:
            adjusted_hour = int(
                event_time_hour) + 12 + hours_to_adjust  # If pm then add 12 hours to adjust to 24 hours format

        # If adjusted_hour < 24 hours no need to update the date
        # if it is over 24 then this means that it is the next day and the date needs to be updated.

        if adjusted_hour < 24:
            adjusted_time = str(
                adjusted_hour) + event_time_minutes  # Returns string representation of the 24h time in HH:MM
            d_of_week = calendar.day_abbr[
                d.weekday()]  # use the calendar API to return Mon-Sun in abbreviated format as a string
            d = d.strftime("%Y.%m.%d")  # Returns the date as a string in the format YYYY:MM:DD
            return d, adjusted_time, d_of_week
        else:
            adjusted_hour = adjusted_hour - 24  # Minus 24h as it is now the next day and 24h time will be am of the next
            # day
            adjusted_time = str(
                adjusted_hour) + event_time_minutes  # Returns string representation of the 24h time in HH:MM
            d = d + timedelta(days=1)  # Adds one day on the original date of the event
            d_of_week = calendar.day_abbr[
                d.weekday()]  # use the calendar API to return Mon-Sun in abbreviated format as a string
            d = d.strftime("%Y.%m.%d")  # Returns the date as a string in the format YYYY:MM:DD
            return d, adjusted_time, d_of_week

        # d = date(year, month, day)

    def send_msg_on_telegram(self, msg):
        telegram_api_url = f"https://api.telegram.org/bot{self.tele_auth_token}/sendMessage?chat_id=@{self.tel_group_id}&text={msg}"
        tel_resp = requests.get(telegram_api_url)
        if tel_resp.status_code == 200:
            print("Notification has been sent on Telegram")
            return True
        else:
            print("Could not send Message")
            return False

    def main(self):
        config = ConfigParser()
        config.read('config.ini')
        print(config['main']['tele_auth_token'])
        print(config['main']['tel_group_id'])
        self.tele_auth_token = config['main']['tele_auth_token']
        self.tel_group_id = config['main']['tel_group_id']
        if self.tele_auth_token is None or self.tel_group_id is None:
            print("Missing Param")
            sys.exit(2)
        event_time_holder = ''

        abs_path = os.path.abspath(__file__)
        cwd = os.path.dirname(abs_path)

        conn = sqlite3.connect('web_scraper.db')
        self.cursor = conn.cursor()
        print("Opened database successfully")

        conn.execute('''CREATE TABLE IF NOT EXISTS FFNEWS
                         (event_id VARCHAR  PRIMARY KEY   NOT NULL,
                         message TEXT NOT NULL,
                         publish_date DATE,
                         title VARCHAR,
                         curreny VARCHAR,
                         actual VARCHAR,
                         forecast VARCHAR,
                         previous VARCHAR,
                         link VARCHAR,
                         status VARCHAR
                         );''')
        print("Table created successfully")

        self.headers = {
            'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/42.0.2311.135 Safari/537.36 Edge/12.246",
            'cookie': "_gid=GA1.2.844585238.1663519562; fflastvisit=1663519951; fflastactivity=0; "
                      "ffsettingshash=5f76e695fd5f0a08d6d06e701906ee66; ffadon=0; sessions_live=1; "
                      "ffmr_thread=1179504:1663556761; flexHeight_flexBox_flex_news_homepageRight1=534; "
                      "flexHeight_flexBox_flex_calendar_mainCalCopy1=485; fftimezoneoffset=undefined; fftimeformat=0; "
                      "ffverifytimes=1; flexHeight_flexBox_flex_minicalendar_=155; "
                      "auth_user=162be527f14ad137c2a50f7b449fca6cf6824de05b6699eb8cbecf8a9a95f071"
                      ":79606f2bb70950181e11e3232a719ca5b9aca847dd57863e5ec803cdaee20f0b; "
                      "flexHeight_flexBox_flex_calendar_mainCal=300; "
                      "__cf_bm=T5lik84SS8u4oJFndPPpIMLPaCGqsaj1nAWfKzEuB5c-1663568923-0"
                      "-ATCtqzamuFRWC8ncBRROjMNLIvZmld01nrHNAw4YC8ObwlDgFAUSCwowgDOFu"
                      "/La93jTZmO5xLXIpCWuYdfd5EXIZDJlmerJiFQIb4ejZzygdNDQL3apHPDHufh9+wU73eegKwg"
                      "/U48rT5bG9HozFMYF1fVd1M97eVg62mfDczHiQ+L6f1Sl8jgSM1D869A4/g==; fftimezone=Asia/Jakarta; "
                      "fftimezoneoffset=7; fftab-history=calendar,index,forums,news; _gat_gtag_UA_3311429_1=1; "
                      "_ga_QFGG4THJR2=GS1.1.1663560654.3.1.1663569555.0.0.0; _ga=GA1.2.629947263.1662711573 "
        }

        while True:
            try:
                self.getEventsCalendarToday()
                sleep(300)
            except requests.exceptions.ProxyError as err:
                print("Proxyyy error: " + str(err))
                continue
            except Exception as e:
                print("There was an error: " + str(e))
                continue
            # getEventsCalendar("calendar?day=sep01.2022", "calendar?day=sep01.2022")
