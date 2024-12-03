# TO DO LIST

* (Опционально) Написать запрос для извлечения уникальных моментов времени из данных по происшествиям 
* (Опционально) Сделать кросс джоин для гексагонов и моментов времени
* Отфильтровать гексагоны по следующим критериям: пересекаются с тульской областью, пересекаются с дорогами или домами 
* Пересечь происшествия с гексагональной сеткой, посчитав количество вхождений
* Проверить, есть ли гескагоны, в которые происшествия не попали вообще
* Посмотреть максимальное количество происшествий в гексагонах
* Для каждого гексагона в соответствие поставить количество попавших происшествий
* Для каждого гескагона поставить в соответствие фичи по объектам инфраструктуры (по количеству)

## Зависимые переменные: 

### ml.iwc, 1 переменная

Происшествия с участием и в отношении несовершеннолетних (см. табл. 29 ЧТЗ)

### ml.kbc 2 переменные

Обращения в отделения полиции с признаком несовершеннолетних КУСП (см. табл. 33 ЧТЗ) 

### ml.dtp_pc, ml.dtp_wsom 4 переменные

ДТП с признаком несовершеннолетних (показатели «ДТП и пострадавшие дети в возрасте до 16 лет и до 18 лет»)

## Фичи по объектам инфраструктуры:

**Слои с объектами (организациями, учреждениями) социальной инфраструктуры:**
- объекты (организации, учреждения) образования, 
  - shelters.stateeducationalorganization
  - shelters.municipaleducationalorganizations 
  - shelters.educationalorganizatsiiimplementing
  - shelters.educationalinstitutionsculture
  - rgis.features  where layer_id=6

- объекты (организации, учреждения) здравоохранения
  - shelters.clinics
  - shelters.dentalclinics
  - shelters.hospitals
  - shelters.childrenshospital
  - shelters.ambulancestation
  - shelters.dispensaries
  - shelters.consultativeanddiagnosticcenters
  - shelters.antenatal
  - shelters.bloodtransfusionstations
  - shelters.sanatoriums
  - shelters.maternities
  - rgis.features  where layer_id=4
  - rgis.features  where layer_id=8
  - rgis.features  where layer_id=9

- объекты (организации, учреждения) физической культуры и спорта,
  - shelters.dyussh
  - shelters.sportivnyezaly  (!)
  - shelters.plavatelnyebass (!)
  - shelters.sportsfields (!)
  - shelters.rdisp
  - rgis.features  where layer_id=7
  - rgis.features  where layer_id=10

**Слои с объектами транспортной инфраструктуры:**

- пешеходные и велосипедные дорожки, пешеходные переходы (см. Рисунок 4),
- маршруты и остановки общественного транспорта,
- автомобильные дороги,
- водные пути (см. Таблица 30), 
- железнодорожные пути (см. Таблица 31),
  - rgis.features  where layer_id=13 
- искусственные дорожные сооружения, 
- комплексные объекты транспортной инфраструктуры,
- линии общественного пассажирского транспорта,
- объекты автомобильного пассажирского транспорта, 
- объекты водного транспорта, 
- объекты воздушного транспорта, 
- объекты железнодорожного транспорта,
- объекты обслуживания и хранения автомобильного транспорта,
- объекты хранения и обслуживания общественного пассажирского транспорта,
- остановочные пункты общественного пассажирского транспорта,
- пункты пропуска через Государственную границу,
- улично-дорожная сеть городского населенного пункта,
- улично-дорожная сеть сельского населенного пункта;
  - rgis.features  where layer_id=11
  - rgis.features  where layer_id=14
  - rgis.features  where layer_id=15
  - rgis.features  where layer_id=16
  - rgis.features  where layer_id=17
  - rgis.features  where layer_id=18
  - rgis.features  where layer_id=20
  - rgis.features  where layer_id=21
  - rgis.features  where layer_id=22
  - rgis.features  where layer_id=23

**Слои с объектами жилищного строительства:**
- текущий жилой фонд (многоквартирные дома, индивидуальные жилые дома),
  - shelters.pvmzhdv2023g (!)
- новостройки (многоквартирные дома), 
- аварийный фонд (многоквартирные дома);
  - shelters.pazhdntto (!)

**Слои с объектами инженерной инфраструктуры:** 65-82
- гидротехнические сооружения, 
- линии электропередачи (ЛЭП), 
- магистральные трубопроводы для транспортировки жидких и газообразных углеводородов, 
- объекты водоотведения, 
- объекты водоснабжения, 
- объекты добычи и транспортировки газа, 
- объекты добычи и транспортировки жидких углеводородов, 
- объекты инженерной защиты от опасных геологических процессов, 
- объекты связи, 
- объекты теплоснабжения, 
- распределительные трубопроводы для транспортировки газа, 
- сети водоотведения,
- сети водоснабжения,
- сети теплоснабжения, 
- сети электросвязи, 
- трубопроводы жидких углеводородов, 
- электрические подстанции, 
- электростанции;
  - rgis.features  where layer_id=27
  - rgis.features  where layer_id=28
  - rgis.features  where layer_id=29
  - rgis.features  where layer_id=30
  - rgis.features  where layer_id=31
  - rgis.features  where layer_id=32
  - rgis.features  where layer_id=33
  - rgis.features  where layer_id=34
  - rgis.features  where layer_id=35
  - rgis.features  where layer_id=36
  - rgis.features  where layer_id=38
  - rgis.features  where layer_id=39
  - rgis.features  where layer_id=40
  - rgis.features  where layer_id=41
  - rgis.features  where layer_id=43
  - rgis.features  where layer_id=44
  - 

**Другое**
- Укрытия
  - shelters.ykritia
- Остановочные пункты
  - kids.oppmto
- Маршруты 
  - kids.mto (!)
- Данные по детям 
  - замужества до 18
    - kids.married_before_18 (!)
  - Привязка к месту жительства и образовательным цчреждениям 
    - kids.children_stat (!)
    - kids.bit_children_union (!)
- Парки, пляжи, скверы
  - rgis.features  where layer_id=3
- Объекты культурно-просветительного назначения
  - rgis.features  where layer_id=5
- Рисковые территории 
  - rgis.features  where layer_id=45
  - 


Отсутствуют в rgis.features  where layer_id=12, 19, 24, 37, 42, 46

Под вопросом: rgis.features  where layer_id=25, 26
