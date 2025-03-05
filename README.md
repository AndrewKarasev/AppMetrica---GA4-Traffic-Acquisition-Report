# AppMetrica GA4 Traffic Acquisition Report
Повтор отчета GA4 Traffic Acquisition на данных из AppMetrica

Данные на выходе: метрики (трафик, заказы, выручка, конверсия) по источнику входа в приложение

![image](https://github.com/user-attachments/assets/073fc91b-f735-4a0f-9c4d-0bd4ea39cf92)

# Описание скрипта
## CTE prepared_sessions
Атрибуция сессий пользователя к какому-либо источнику входа по методу Last Non-Direct Click. Окно атрибуции - 90 дней. Документация по ручному формированию отчета GA4 Traffic Acquisition: https://tanelytics.com/ga4-bigquery-session-traffic_source/
### CTE sources_union
Объединение Direct и Non-Direct сессиий. Исключаем Direct сессии, если в той же минуте присутствует Non-Direct сессиии.
#### CTE non_direct_sessions
Non-Direct сессии это пуши и вход по https ссылке (с последующим преобразованием в deeplink). Событие открытия приложения по пушу - PushOpen. Событие открытия приложения по https ссылке - WebLinkOpen. В событии WebLinkOpen собираются utm-метки и другие параметры ссылки для более точного распознавания источника. Для получения таких событий используем выгрузку Logs API /logs/v1/export/events.
#### CTE direct_sessions
По-умолчанию считаем Direct все сессии в AppMetrica. Используем выгрузку Logs API /logs/v1/export/sessions_starts. Логически Direct вход означает самостоятельное открытие приложение пользователем.
### CTE attributed_sessions
### CTE profiles
Получение client_id - уникального идентификатора из CRM. Используем выгрузку Logs API /logs/v1/export/profiles_v2.
## CTE orders_crm
Получение заказов из CRM. Можно использовать заказы из Logs API /logs/v1/export/events.

## Итог
