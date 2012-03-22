# -*- coding: utf-8 -*-

from threading import Thread
from time import sleep

class CountUpdater(Thread):
    def init_app(self, files_store, recalc_interval):
        self.files_store = files_store
        self.recalc_interval = recalc_interval
        self.lastcount = files_store.count_files()
        self.daemon = True
    
    def run(self):
        while True:
            self.lastcount = int(self.files_store.count_files())
            sleep(self.recalc_interval)


