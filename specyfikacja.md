# Specyfikacja aplikacji

CzyPrzyjedzie to aplikacja która ma służyć do śledzenia pojazdów komunikacji miejskiej w czasie rzeczywistym.

## 1. Funkcjonalności techniczne

- Aplikacja może obsługiwać wiele miast naraz, użytkownik wybiera dla którego chce zobaczyć komunikację miejską na stronie głównej
- Po wejściu na stronę danego miasta pokazuje się mapa, na której są przystanki oraz pojazdy
- Użytkownik może zobaczyć teorytyczne, oraz rzeczywiste odjazdy z każdego przystanku
- Użytkownik może zobaczyć trasę każdego pojazdu na mapie
- Użytkownik może zobaczyć szczegóły trasy każdej linii w danym mieście, oraz wszystkie brygady¹ które obsługują tą linię

¹ - Brygada - zaplanowany ciąg kursów wykonywanych przez ten sam pojazd

Aplikacja w celu pokazania tych danych użytkownikowi korzysta z własnego API, które konwertuje dane w formacie GTFS (format danych dla transportu publicznego - https://gtfs.org/) na pliki JSON.

# Dokumentacja API
Dokumentacja znajduje się pod linkiem: https://docs.google.com/document/d/1PMuvmuOLzl3F9kbQS5fQFDpUSID7lS8-gvPQ5M81UXQ/edit?usp=sharing
