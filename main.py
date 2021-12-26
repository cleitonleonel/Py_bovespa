#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#
import os
import csv
import json
import requests
from bs4 import BeautifulSoup

URL_BASE = 'https://opcoes.net.br/opcoes/bovespa/'
BASE_DIR = os.getcwd()

LIST_MENU_BA = ['BBAS3', 'BBDC3', 'BBDC4', 'BBTG11', 'BRSR6', 'ITSA4', 'ITUB3', 'ITUB4', 'SANB11']

LIST_MENU_ML = ['ABEV3', 'AZUL4', 'B3SA3', 'BBAS3', 'BBDC3', 'BBDC4', 'BBSE3', 'BOVA11', 'BRAP4', 'BRFS3', 'BRKM5', 'BRML3',
                'BTOW3', 'CIEL3', 'CMIG4', 'COGN3', 'CSAN3', 'CSNA3', 'CYRE3', 'ELET3', 'ELET6', 'EMBR3', 'EQTL3', 'GGBR4',
                'GOAU4', 'HYPE3', 'IRBR3', 'ITSA4', 'ITUB4', 'JBSS3', 'LAME4', 'LREN3', 'MGLU3', 'MRFG3', 'PCAR3', 'PETR3',
                'PETR4', 'RADL3', 'RAIL3', 'SANB11', 'SBSP3', 'SUZB3', 'TAEE11', 'USIM5', 'VALE3', 'VIVT4', 'VVAR3', 'WEGE3', 'YDUQ3'
                ]

LIST_MENU_PV = ['PETR3', 'PETR4', 'VALE3', 'VALE5']

LIST_MENU_PO = ['ABEV3', 'AZUL4', 'B3SA3', 'BBAS3', 'BBDC3', 'BBDC4', 'BBSE3', 'BEEF3', 'BOVA11', 'BRAP4', 'BRDT3', 'BRFS3',
                'BRKM5', 'BRML3', 'BRSR6', 'BTOW3', 'CCRO3', 'CIEL3', 'CMIG4', 'COGN3', 'CSAN3', 'CSNA3', 'CVCB3', 'CYRE3',
                'ECOR3', 'EGIE3', 'ELET3', 'ELET6', 'EMBR3', 'ENAT3', 'ENBR3', 'EQTL3', 'FLRY3', 'GFSA3', 'GGBR4', 'GOAU4',
                'GOLL4', 'HYPE3', 'IRBR3', 'ITSA4', 'ITUB4', 'JBSS3', 'JHSF3', 'KLBN11', 'LAME4', 'LREN3', 'MEAL3', 'MGLU3',
                'MRFG3', 'MRVE3', 'MULT3', 'NEOE3', 'PCAR3', 'PETR3', 'PETR4', 'POMO4', 'PRIO3', 'RADL3', 'RAIL3', 'SANB11',
                'SBSP3', 'SMLS3', 'SUZB3', 'TAEE11', 'TIET11', 'TIMS3', 'UGPA3', 'USIM5', 'VALE3', 'VIVT4', 'VLID3', 'VVAR3',
                'WEGE3', 'YDUQ3'
                ]

LIST_MENU_TA = ['ABEV3', 'ALPA4', 'ALSO3', 'ALUP11', 'ARZZ3', 'AZUL4', 'B3SA3', 'BBAS3', 'BBDC3', 'BBDC4', 'BBSE3', 'BEEF3', 'BKBR3', 'BOVA11',
                'BOVV11', 'BPAC11', 'BPAN4', 'BRAP4', 'BRDT3', 'BRFS3', 'BRKM5', 'BRML3', 'BRSR6', 'BTOW3', 'CCRO3', 'CESP6', 'CIEL3', 'CMIG4',
                'COGN3', 'CPFE3', 'CPLE6', 'CRFB3', 'CSAN3', 'CSMG3', 'CSNA3', 'CVCB3', 'CYRE3', 'DIRR3', 'DTEX3', 'ECOR3', 'EGIE3', 'ELET3', 'ELET6',
                'EMBR3', 'ENAT3', 'ENBR3', 'ENEV3', 'ENGI11', 'EQTL3', 'EVEN3', 'EZTC3', 'FLRY3', 'GFSA3', 'GGBR4', 'GNDI3', 'GOAU4', 'GOLL4', 'GRND3',
                'HAPV3', 'HBOR3', 'HGTX3', 'HYPE3', 'IBOV11', 'IGTA3', 'IRBR3', 'ITSA4', 'ITUB3', 'ITUB4', 'IVVB11', 'JBSS3', 'JHSF3', 'KLBN11',
                'LAME3', 'LAME4', 'LCAM3', 'LIGT3', 'LINX3', 'LOGN3', 'LREN3', 'LWSA3', 'MDIA3', 'MEAL3', 'MGLU3', 'MOVI3', 'MRFG3', 'MRVE3',
                'MULT3', 'MYPK3', 'NEOE3', 'NTCO3', 'ODPV3', 'PCAR3', 'PCAR4', 'PETR3', 'PETR4', 'POMO4', 'PRIO3', 'PSSA3', 'QUAL3', 'RADL3',
                'RAIL3', 'RAPT4', 'RENT3', 'SANB11', 'SAPR11', 'SBSP3', 'SEER3', 'SMAL11', 'SMLS3', 'SMTO3', 'STBP3', 'SULA11', 'SUZB3', 'TAEE11',
                'TIET11', 'TIMP3', 'TIMS3', 'TOTS3', 'TRPL4', 'TUPY3', 'UGPA3', 'USIM5', 'VALE3', 'VIVT4', 'VLID3', 'VVAR3', 'WEGE3', 'YDUQ3'
                ]


class Browser(object):

    def __init__(self):
        """
        Inicia o objeto...

        """
        self.response = None
        self.session = requests.Session()
        self.soup_parser = {'features': 'html5lib'}

    def headers(self):
        """
        Obtem o cabeçalho inicial da requisição.

        :return:
        """
        headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:72.0)'
                          ' Gecko/20100101 Firefox/72.0',
        }
        return headers

    def send_request(self, url, method, soup_cnf, **kwargs):
        """
        Envia a requisição GET salvando a sessão para consulta posterior.
        Retorna com as informações da consulta e o código fonte da página.

        :param url:
        :param method:
        :param kwargs:
        :return:
        """
        try:
            response = self.session.request(method, url, **kwargs)
        except ValueError:
            return None
        if response.status_code == 200:
            try:
                return self.format_html(response.text, soup_config=soup_cnf)
            except ValueError:
                return []

    def format_html(self, response, soup_config=None):
        """
        Formata o html para possibilitar a raspagem de dados.

        :param response:
        :param soup_config:
        :return:
        """

        self.soup_parser = soup_config or self.soup_parser
        soup = BeautifulSoup(response, **self.soup_parser)
        return soup


class Bovespa(Browser):

    def __init__(self):
        super().__init__()
        self.active = ''
        self.due_date = None
        self.date_hour = None
        self.list_menu = None
        self.current_json = None

    def get_actives(self, list_menu='ML'):
        """
        Define o tipo de ação a ser consultada e retorna uma lista de ativos.

        :param list_menu:
        :return:
        """

        list_actives = []

        if list_menu != 'ML':
            if list_menu == 'BA':
                list_actives = LIST_MENU_BA
            elif list_menu == 'PV':
                list_actives = LIST_MENU_PO
            elif list_menu == 'PO':
                list_actives = LIST_MENU_PV
            elif list_menu == 'TA':
                list_actives = LIST_MENU_TA
        else:
            list_actives = LIST_MENU_ML

        return list_actives

    def get_due_dates(self):
        """
        Faz a consulta de vencimentos disponíveis e retorna uma lista.

        :return:
        """

        list_due = []
        for due_dt in self.current_json['data']['expirations']:
            list_due.append(due_dt['dt'])
        return list_due

    def get_json(self, active, soup_cnf=None):
        """
        Recebe um ativo e faz a consulta retornando um json extraído do código fonte.

        :param active:
        :param soup_cnf:
        :return:
        """

        self.active = active
        response = self.send_request(URL_BASE + f'{self.active}/json', 'GET', soup_cnf=soup_cnf)
        if soup_cnf:
            self.current_json = json.loads(response.text)
            return self.current_json
        json_data = json.loads(response.find('body').text)
        self.current_json = json_data

    def get_calls(self):
        """
        Itera na lista de operações e retorna uma lista de ações de compra.

        :return:
        """

        call_list = []
        for call in self.current_json['data']['expirations']:
            if self.due_date:
                if self.due_date == call['dt']:
                    call_list.append(call['calls'])
            else:
                call_list.append(call['calls'])
        if self.date_hour:
            return self.date_hour_aplly(call_list)
        else:
            new_call_list = []
            for action in call_list[0]:
                new_call_list.append(action)
            call_list = new_call_list
        return call_list

    def get_puts(self):
        """
        Itera na lista de operações e retorna uma lista de ações de venda.

        :return:
        """

        put_list = []
        for put in self.current_json['data']['expirations']:
            if self.due_date:
                if self.due_date == put['dt']:
                    put_list.append(put['puts'])
            else:
                put_list.append(put['puts'])
        if self.date_hour:
            return self.date_hour_aplly(put_list)
        else:
            new_put_list = []
            for action in put_list[0]:
                new_put_list.append(action)
            put_list = new_put_list
        return put_list

    def set_filters(self, due_date=None, date_hour=None):
        """
        Define o filtro ou filtros a serem aplicados.

        :param due_date:
        :param date_hour:
        :return:
        """

        if due_date:
            self.due_date = due_date
        if date_hour:
            self.date_hour = date_hour

    def date_hour_aplly(self, data):
        """
        Aplica o filtro de data/hora no objeto e itera nele retornando uma lista de ações.

        :param data:return:
        """

        list_actions = []
        for action in data[0]:
            for index, item in enumerate(action):
                if self.date_hour:
                    if index == 7:
                        if item == self.date_hour:
                            list_actions.append(action)
        return list_actions

    def export_to_csv(self, type, data):
        """
        Exporta, ou seja, cria um arquivo csv conforme o tipo.

        :param type:
        :param data:return:
        """

        print(f'\nGerando {type} CSV file...')

        cols = ['TICKER', '', 'ESTILO', 'STRIKE', 'A/I/O', 'ÚLTIMO',
                'VARIAÇÃO', 'DATA', 'NEGÓCIOS', 'VOL.FINANCEIRO', 'VOLATILIDADE',
                'DELTA', 'GAMMA', 'THETA($)', 'THETA(%)', 'VEGA',
                ]

        file_path = os.path.join(BASE_DIR, 'src/data')
        if not os.path.exists(file_path):
            os.makedirs(file_path, exist_ok=True)

        with open(f"{file_path}/{type}.csv", "w", encoding='UTF-8') as f:
            writer = csv.writer(f, delimiter=",", lineterminator="\n")
            writer.writerow(cols)
            for item in data:
                writer.writerow(item)

        print(f'\n{type} CSV criado com sucesso.')


if __name__ == '__main__':
    bv = Bovespa()  # Inicia o objeto.

    #  Recebe um filtro para selecão de ativos
    actives = bv.get_actives(list_menu='TA')  # Retorna a uma lista com os ativos referente ao tipo selecionado, por padrão usa 'ML', 'TA' traz todos.
    # print('\nLISTA DE ATIVOS: ', actives)

    """
    #  Diz ao objeto qual o ativo que queremos extrair dados
    result = bv.get_json(actives[0], soup_cnf={"features": "html.parser"})  # Pega os dados do ativo da posição 0 na lista de ativos, mas outros podem ser selecionados.
    # print(json.dumps(result, indent=4))

    #  Chama o método que extrai o filtro de datas de vencimentos
    dues = bv.get_due_dates()  # Retorna uma lista com os filtros de vencimentos para serem aplicados
    # print('\nLISTA DE VENCIMENTOS: ', dues)

    #  Aplica o filtro de vencimentos e ou data/hora, aguarda por sub-filtros de compra ou venda
    bv.set_filters(due_date=dues[0])  # Define os filtros para a busca seja de compra ou venda de um ativo, o filtro de vencimento deve ser passado com índice referente

    #  Chama o método que traz todas as operações de compra, ainda sem filtros aplicados
    calls = bv.get_calls()  # Retorna uma lista com todas as operações de compra.
    # print(calls)

    #  Chama o método que traz todas as operações de compra, ainda sem filtros aplicados
    puts = bv.get_puts()  # Retorna uma lista com todas as operações de venda.
    # print(puts)

    print('')

    for action in calls:
        print('CALL: ', action)

    print('*' * 150)

    for action in puts:
        print('PUT: ', action)
        
    bv.export_to_csv('PUT', puts)
    bv.export_to_csv('CALL', calls)
    """

    for active in actives:
        result = bv.get_json(active, soup_cnf={"features": "html.parser"})
        dues = bv.get_due_dates()
        if len(dues) > 0:
            bv.set_filters(due_date=dues[0])

            calls = bv.get_calls()
            puts = bv.get_puts()

            bv.export_to_csv(f'PUT_for_{active}', puts)
            bv.export_to_csv(f'CALL_for_{active}', calls)
