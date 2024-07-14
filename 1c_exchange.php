<?

use \Bitrix\Main\Loader,
    \Bitrix\Crm,
    \Bitrix\Crm\CompanyTable;

require($_SERVER["DOCUMENT_ROOT"] . "/bitrix/modules/main/include/prolog_before.php");

class Rest1C
{

    public static $login = "XAXAXA";
    public static $password = "XAXAXA";
    public static $HL_syncID = 3;
    public static $HL_sync_contactID = 4;
    public $entity_data_class = false;
    public $entity_data_class_contact = false;
    private $arraySyncHL;//массив для хранения данных из HL
    private $arraySyncContactsHL;//массив для хранения данных из HL
    public $matchFieldsCompany = [
        "ВариантОтправкиЭлектронногоЧека" => "UF_ELECT_RECEIPTION",
        "ДатаРегистрации" => "UF_DATE_TIME_REG",
        "Комментарий" => "COMMENTS",
        "НазначениеПереработчика" => "UF_PEREOBR_DESTINATION",
        "НазначениеПереработчикаGUID" => "UF_PEREOBR_DESTINATION_GUID",
        "Поставщик" => "UF_SUPPLIES",
        "_Город" => "UF_SUPPLIES",
        "_ГородGUID" => "UF_CITY_GUID",
        "_Организация" => "UF_ORGANIZATION",
        "_ОрганизацияGUID" => "_ОрганизацияGUID",
        "_СегментОсновной" => "UF_SEGMENT",
        "_СегментОсновнойGUID" => "UF_SEGMENT_GUID",
    ];

    public function __construct()
    {
        Loader::includeModule('highloadblock');
        //компилируем сущность для работы с таблицей синхронизации
        $this->entity_data_class = Bitrix\Highloadblock\HighloadBlockTable::compileEntity(Bitrix\Highloadblock\HighloadBlockTable::getById(self::$HL_syncID)->fetch())->getDataClass();
        $this->entity_data_class_contact = Bitrix\Highloadblock\HighloadBlockTable::compileEntity(Bitrix\Highloadblock\HighloadBlockTable::getById(self::$HL_sync_contactID)->fetch())->getDataClass();
    }

    static function sendRequst($url = '', $query = [])
    {
        if ($query) {
            $query = json_encode($query);
        }

        if ($objCurl = curl_init()) {
            curl_setopt_array(
                $objCurl,
                [
                    CURLOPT_HTTPHEADER => ['Content-Type: application/json'],
                    CURLOPT_SSL_VERIFYPEER => false,
                    CURLOPT_SSL_VERIFYHOST => false,
                    CURLOPT_POSTREDIR => 10,
                    CURLOPT_POST => true,
                    CURLOPT_HEADER => false,
                    CURLOPT_RETURNTRANSFER => true,
                    CURLOPT_URL => $url,
                    CURLOPT_POSTFIELDS => $query,
                    CURLOPT_TIMEOUT => 60,
                    CURLOPT_HTTPAUTH => CURLAUTH_ANY,
                    CURLOPT_USERPWD => self::$login . ":" . self::$password,
                    CURLOPT_CUSTOMREQUEST => 'GET',
                ]
            );
            $result = curl_exec($objCurl);
            if ($result === false) {
                $error = 'CURL error: ' . curl_error($objCurl);
                pr($error);
            } else {
                $result = json_decode($result, 1);
                $result = (array)$result;
            }
            curl_close($objCurl);
        }


        return $result;
    }



    // получение списка
    // $combineResults - все результаты предыдущих выборок
    // $step - если true, то работаем по шагам, т.е. получаем результат конткретной страницы
    static function getList($method, $array = false, $start = false, $step = false, $combineResults = false)
    {
        $query = ($array) ? $array : array();

        if ($start) {
            $query = array("Number" => $start);
            $start++;
        }

        $result = self::sendRequst($method, $query);

        //pr($result, 1);

        // пошаговая работа
        if ($step) {
            if (is_array($result[0])) {
                // если пришли данные в текущей итерации, то формируем следующий шаг
                $result["NEXT"] = $start;
            } else {
                // если не пришли, то возвращаем финиш
                $result["FINISH"] = "Y";
            }
            return $result;
        }

        // если получили массив элементов, то собираем данные
        if (is_array($result[0])) {
            foreach ($result as $el) {
                $combineResults[] = $el;
            }

            //sleep(1); // разгрузим API сделав паузу
            // выполняем следующий шаг
            if ($start) {
                echo $start;
                $result = self::getList($method, $array, $start, false, $combineResults);
                return $result;
            }
        } else {
            echo "Закончили на " . $start . " шаг";
            die();
            return $combineResults;
        }

        return $combineResults;
    }

    // добавление записи в 1С (используется для создания интереса)
    static function add($method, $array)
    {
        $result = self::sendRequst($method, $array);
        return $result;
    }


    //наполняем таблицу для проведенеия синхронизации
    //функция добавляет/изменияет данные о компаниях в ХЛ
    public function fillingSyncTable()
    {
        $nextStep = 1;
        $timeStart = \Bitrix\Main\Type\DateTime::createFromTimestamp(time());
        $cntUpdate = 0;
        $cntAdd = 0;
        $cnt1CRecords = 0;

        while (1 == 1) {
            unset($arResult);
            $arResult["LOG"][] = "Время старта $timeStart. Текущий шаг ".$nextStep;

            $arrayData = Rest1C::getList("https://10.116.20.40/ka/hs/Bitrix/getPartners", false, $nextStep, true);
            $nextStep = $arrayData["NEXT"];
            unset($arrayData["NEXT"]);

            if(isset($arrayData["FINISH"])){
                break;
            }

            $cnt1CRecords += count($arrayData);

            $arResult["LOG"][] = "Получили данных из 1С ".$cnt1CRecords. " записей";

            //1. получаем весь список компаний у нас в HL
            if ($this->entity_data_class) {
                $obData = $this->entity_data_class::getList(
                    array(
                        "select" => array("ID", "UF_GUID", "UF_HASH", "UF_PROCESS_FLAG"),
                        'order' => ['ID' => 'ASC'],
                    )
                );

                while ($array = $obData->Fetch()) {
                    //если не нашли у себя в массиве эти данные - добавляем из для упрощения поиска
                    if (!isset($this->arraySyncHL[$array["UF_GUID"]]) && $array["UF_GUID"]) {
                        $this->arraySyncHL[$array["UF_GUID"]] = $array;
                    }
                }
            }

            //2. перебираем массив $arrayData и смотрим, есть ли запись у нас в БД.
            foreach ($arrayData as $array) {
                if (!$array["ПартнерGUID"]) {
                    continue;
                }

                //если записи нет - добавляем ее и устанавливаем флаг в поле UF_PROCESS_FLAG в U - на обновление
                if (!isset($this->arraySyncHL[$array["ПартнерGUID"]])) {
                    $arHlData = [
                        "UF_1C_RAW_DATA" => $this->getRawData($array),
                        "UF_TIMESTAMP" => \Bitrix\Main\Type\DateTime::createFromTimestamp(time()),
                        "UF_GUID" => $array["ПартнерGUID"],
                        "UF_HASH" => $this->getHash(serialize($array)),
                        "UF_PROCESS_FLAG" => "U"
                    ];
                    $res = $this->entity_data_class::add($arHlData);

                    $this->arraySyncHL[$array["ПартнерGUID"]] = [
                        "UF_GUID" => $array["ПартнерGUID"],
                        "UF_HASH" => $this->getHash(serialize($array)),
                        "ID" => $res->getId(),
                    ];
                } //если запись есть - проверяем ее хеш -
                else {
                    //если хеш не совпадает, то это означает, что прилетели изменения, устанавливаем флаг в U и сохраняем их в БД
                    if ($this->arraySyncHL[$array["ПартнерGUID"]]["UF_HASH"] != $this->getHash(serialize($array))
                        &&
                        ($this->arraySyncHL[$array["ПартнерGUID"]]["UF_PROCESS_FLAG"] == "U"
                            || $this->arraySyncHL[$array["ПартнерGUID"]]["UF_PROCESS_FLAG"] == "")
                    ) {
                        $arHlData = [
                            "UF_PROCESS_FLAG" => "U",
                            "UF_TIMESTAMP" => \Bitrix\Main\Type\DateTime::createFromTimestamp(time()),
                            "UF_HASH" => $this->getHash(serialize($array)),
                            "UF_1C_RAW_DATA" => $this->getRawData($array),
                        ];

                        $this->entity_data_class::update($this->arraySyncHL[$array["ПартнерGUID"]]["ID"], $arHlData);
                    }
                }
            }
            $cntAllUpdate = $this->checkUpdate();
            $arResult["LOG"][] = "Всего на обновлении $cntAllUpdate записей";

            COption::SetOptionString("main", "cntFillingCompany", implode(".<br />", $arResult["LOG"]));
        }
        $cntAllUpdate = $this->checkUpdate();
        COption::SetOptionString("main", "cntFillingCompany", "Закончили заполнение промежуточной таблицы для апдейта ". \Bitrix\Main\Type\DateTime::createFromTimestamp(time()) .". Всего на обновление $cntAllUpdate записей.");
    }

    public
    function actionUpdateCompany()
    {
        $arResult = [];
        if (Loader::includeModule("crm")) {
            $limit = 500;
            $cnt = 0;
            $exit = false;
            $start = microtime(true);
            $arContactParams = array(
                "Телефон" => array("TYPE" => "PHONE"),
                "Адрес электронной почты" => array("TYPE" => "EMAIL"),
            );
            while (!$exit) {
                unset($dataCollection);
                unset($res);
                unset($arFilterGUID);
                unset($arCompanyID);
                //1. получаем первые N компаний из таблицы синхронизации, которые стоят на очереди на апдейт (UF_PROCESS_FLAG = U)
                if ($this->entity_data_class) {
                    $cnt++;
                    $res = $this->entity_data_class::getList(
                        [
                            "select" => ["ID", "UF_GUID", "UF_1C_RAW_DATA"],
                            'order' => ['ID' => 'ASC'],
                            'filter' => [
                                'LOGIC' => 'OR',
                                "UF_PROCESS_FLAG" => "U",
                                //считаем, что если записи висят в обработке дольше 10 минут, то предъидущий процесс упал и не доделал дело
                                //захватываем их на борт лайнера "обработка" в этот проход
                                [
                                    'LOGIC' => 'AND',
                                    'UF_PROCESS_FLAG' => "P",
                                    "<=UF_TIMESTAMP" => \Bitrix\Main\Type\DateTime::createFromTimestamp(strtotime("-10 min"))
                                ]
                            ],
                            'limit' => $limit
                        ]
                    );
                    $dataCollection = $res->fetchCollection();

                    //если больше нечего обрабатывать выходим
                    if ($dataCollection->isEmpty()) {
                        $exit = true;

                        $arResult["FINISH"] = "1";
                        $arResult["in_process"] = $this->checkUpdate();

                        echo json_encode($arResult);
                        break;
                    }

                    $arFilterGUID = [];
                    //собираем данные для фильтра по компаниям, чтобы найти их в нашей БД
                    foreach ($dataCollection as $elementCollection) {
                        $ar = [];
                        $ar["ID"] = $elementCollection->get("ID");
                        $ar["UF_GUID"] = $elementCollection->get("UF_GUID");
                        $ar["UF_1C_RAW_DATA"] = $elementCollection->get("UF_1C_RAW_DATA");

                        $arFilterGUID[$elementCollection->get("UF_GUID")] = $ar;

                        $elementCollection->set("UF_PROCESS_FLAG", "P");
                        $elementCollection->set("UF_TIMESTAMP", \Bitrix\Main\Type\DateTime::createFromTimestamp(time()));
                    }
                    $res = $dataCollection->save(true);

                    if (!$res->isSuccess()) {
                        echo "Ошибка сохранения флагов обработки";
                        return false;
                    }

                    // проверим наличие компании в CRM
                    $dbFields = CCrmCompany::GetList(
                        [],
                        [
                            "ORIGIN_ID" => array_keys($arFilterGUID),
                            ["ID"],
                            false
                        ]
                    );

                    while ($arResultCompany = $dbFields->Fetch()) {
                        $arCompanyID[$arResultCompany["ORIGIN_ID"]] = $arResultCompany["ID"];
                    }

                    //получаем всех менеджеров компании
                    $arUsers = $this->getBXUsers();

                    //бежим по фильтру и ищем записи у нас
                    foreach ($arFilterGUID as $strGUID => $arData) {
                        $contragent = unserialize($arData["UF_1C_RAW_DATA"]);
                        if (!is_array($contragent)) {
                            continue;
                        }

                        //сохраняем обязательные поля
                        $arCompany = array(
                            "ORIGIN_ID" => $contragent["ПартнерGUID"],
                            "ORIGINATOR_ID" => "1C",
                            "TITLE" => $contragent["НаименованиеПолное"],
                            "COMPANY_TYPE" => "PARTNER",
                            "ASSIGNED_BY_ID" => isset($arUsers[$contragent["ОсновнойМенеджерGUID"]]) ? $arUsers[$contragent["ОсновнойМенеджерGUID"]] : 1,
                            "UF_COMPANY_TYPE" => ($contragent["ЮрФизЛицо"] == "Физическое лицо") ? 42 : 41, // юрлицо или физлицо
                            "OPENED" => "Y",
                        );

                        //сохраняем поля из настроек
                        foreach ($contragent as $str1CName => $value) {
                            if (isset($this->matchFieldsCompany[$str1CName])) {
                                $arCompany[$this->matchFieldsCompany[$str1CName]] = $value;
                            }
                        }

                        // обрабатываем контакнтую информацию
                        foreach ($contragent["ТабличныеЧасти"][0]["КонтактнаяИнформация"] as $contacts) {
                            if ($contacts["Тип"] == "Адрес") {
                                $addressType = "ADDRESS";
                                if ($contacts["Вид"] == "Юридический адрес") {
                                    $addressType = "REG_ADDRESS";
                                }
                                $arCompany[$addressType] = $contacts["Представление"];
                            } else {
                                $arCompany["FM"][$arContactParams[$contacts["Тип"]]["TYPE"]] = array(
                                    "n" . $nKey++ => array(
                                        "VALUE_TYPE" => "WORK",
                                        "VALUE" => $contacts["Представление"],
                                    )
                                );
                            }
                        }

                        $company = new CCrmCompany(false);
                        if (isset($arCompanyID[$strGUID])) {
                            $company->Update($arCompanyID[$strGUID], $arCompany);
                        } else {
                            $companyID = $company->Add($arCompany);
                            $arCompanyID[$strGUID] = $companyID;
                        }

                        $this->entity_data_class::update(
                            $arData["ID"],
                            [
                                "UF_PROCESS_FLAG" => "",
                                "UF_TIMESTAMP" => \Bitrix\Main\Type\DateTime::createFromTimestamp(time())
                            ]
                        );
                    }
                    //спим секунду, чтобы дать остыть АПИ
                    sleep(1);

                    $arResult["NEXT"] = 1;
                    $cntUpdate = $this->checkUpdate();
                    $arResult["LOG"] = "Шаг $cnt. Обновили $limit компаний за " . round(microtime(true) - $start, 4) . " секунд. Осталось обновить " . $cntUpdate . ". Займет примерно " . round($cntUpdate / $limit) . " шагов.";

                    COption::SetOptionString("main", "cntUpdate", $arResult["LOG"]);
                }
            }
        }
    }

    public
    function checkUpdate()
    {
        $count = count(
            $this->entity_data_class::getList(
                [
                    "select" => ["ID"],
                    'order' => ['ID' => 'ASC'],
                    'filter' => [
                        'LOGIC' => 'OR',
                        ["UF_PROCESS_FLAG" => "U"],
                        ["UF_PROCESS_FLAG" => "P"],
                    ]
                ]
            )->fetchAll()
        );

        return $count;
    }


    public
    function checkUpdateContacts()
    {
        $count = count(
            $this->entity_data_class_contact::getList(
                [
                    "select" => ["ID"],
                    'order' => ['ID' => 'ASC'],
                    'filter' => [
                        'LOGIC' => 'OR',
                        ["UF_PROCESS_FLAG" => "U"],
                        ["UF_PROCESS_FLAG" => "P"],
                    ]
                ]
            )->fetchAll()
        );

        return $count;
    }


    public
    function getHash(
        $array
    ) {
        return hash("sha256", serialize($array), false);
    }

    public
    function getRawData(
        $array
    ) {
        return serialize($array);
    }

    //вернем всех пользователей. Ключ массива GUID, значение - ID в BX
    public function getBXUsers()
    {
        $arParams["FIELDS"] = array("ID", "UF_1C_GUID");
        $arParams["SELECT"] = array("UF_1C_GUID");
        $filter = array("ACTIVE" => "Y", "!UF_1C_GUID" => false);
        $rsUsers = CUser::GetList(($by = "id"), ($order = "desc"), $filter, $arParams);
        while ($arUser = $rsUsers->GetNext()) {
            $arResult[$arUser["UF_1C_GUID"]] = $arUser["ID"];
        }

        return $arResult;
    }

    public function getCompany()
    {
        $res =
            $this->entity_data_class::getList(
                [
                    "select" => ["*"],
                    'order' => ['ID' => 'ASC'],
                    'limit' => 1000
                ]
            );

        while ($ar = $res->fetch()) {
            $arr = unserialize($ar["UF_1C_RAW_DATA"]);
            $ar["UF_1C_RAW_DATA"] = $arr;
            $arT[] = $ar;
        }

        return $arT;
    }

    public function fillingSyncTableContacts()
    {
        //1. так как 1Ска отдает контакты только по фильтру компаний, то нам нужно в запросе передавать гуиды компаний.
        $limit = 1000;
        $offset = 1;
        $allCountCompany = 0;

        $timeStart = \Bitrix\Main\Type\DateTime::createFromTimestamp(time());

        $start = microtime(true);

        while (1 == 1) {
            $res =
                $this->entity_data_class::getList(
                    [
                        "select" => ["ID", "UF_GUID"],
                        'order' => ['ID' => 'ASC'],
                        'limit' => $limit,
                        'offset' => $offset * $limit + 1
                    ]
                );

            unset($arrayGUID);
            $arT = [];
            while ($arCompany = $res->fetch()) {
                $arrayGUID["ПартнерGUID"][] = $arCompany["UF_GUID"];
                $allCountCompany++;

            }

            //если пустой массив с гуидами выходим
            if (!$arrayGUID) {
                break;
            }

            $arContacts = Rest1C::getList("https://10.116.20.40/ka/hs/Bitrix/getContactfaces", $arrayGUID, false, true);
            $cnt1CContacts = count($arContacts);


            //получаем всю таблицу контактов если на первом шаге и $offset == 1
            if ($this->entity_data_class_contact && $offset == 1) {
                $obData = $this->entity_data_class_contact::getList(
                    array(
                        "select" => array("ID", "UF_GUID", "UF_HASH", "UF_PROCESS_FLAG"),
                        'order' => ['ID' => 'ASC'],
                    )
                );

                while ($array = $obData->Fetch()) {
                    //если не нашли у себя в массиве эти данные - добавляем из для упрощения поиска
                    if (!isset($this->arraySyncContactsHL[$array["UF_GUID"]]) && $array["UF_GUID"]) {
                        $this->arraySyncContactsHL[$array["UF_GUID"]] = $array;
                    }
                }
            }

            //сдвигаем offset
            $offset++;


            foreach ($arContacts as $array) {
                if (!$array["СсылкаGUID"]) {
                    continue;
                }

                //если в таблице синхронизации нет текущего контакта нам нужно его туда добавить
                if (!isset($this->arraySyncContactsHL[$array["СсылкаGUID"]])) {
                    $arHlData = [
                        "UF_TIMESTAMP" => \Bitrix\Main\Type\DateTime::createFromTimestamp(time()),
                        "UF_1C_RAW_DATA" => $this->getRawData($array),
                        "UF_GUID" => $array["СсылкаGUID"],
                        "UF_HASH" => $this->getHash(serialize($array)),
                        "UF_PROCESS_FLAG" => "U",
                    ];
                    $res = $this->entity_data_class_contact::add($arHlData);

                    $this->arraySyncContactsHL[$array["ПартнерGUID"]] = [
                        "UF_GUID" => $array["СсылкаGUID"],
                        "UF_HASH" => $this->getHash(serialize($array)),
                        "ID" => $res->getId(),
                    ];
                } //если контакт в таблице есть и не совпадает хеш - обновляем данные
                else {
                    //если хеш не совпадает, то это означает, что прилетели изменения, устанавливаем флаг в U и сохраняем их в БД
                    if ($this->arraySyncContactsHL[$array["ПартнерGUID"]]["UF_HASH"] != $this->getHash(serialize($array))
                        &&
                        ($this->arraySyncContactsHL[$array["ПартнерGUID"]]["UF_PROCESS_FLAG"] == "U"
                            || $this->arraySyncContactsHL[$array["ПартнерGUID"]]["UF_PROCESS_FLAG"] == "")
                    ) {
                    $arHlData = [
                        "UF_PROCESS_FLAG" => "U",
                        "UF_TIMESTAMP" => \Bitrix\Main\Type\DateTime::createFromTimestamp(time()),
                        "UF_HASH" => $this->getHash(serialize($array)),
                        "UF_1C_RAW_DATA" => $this->getRawData($array),
                    ];

                    $this->entity_data_class_contact::update($this->arraySyncContactsHL[$array["СсылкаGUID"]]["ID"], $arHlData);
                    }
                }
                $cntUpdate = $this->checkUpdateContacts();
                COption::SetOptionString("main", "cntUpdateContacts", "Получили $cnt1CContacts записей из 1С. на обновление стоит $cntUpdate записей. Текущий оффсет $offset");
            }
        }
        $this->actionUpdateCompanyContacts();
        COption::SetOptionString("main", "cntUpdateContacts", "Закончили выгрузку контактов из 1С во временную таблицу");
    }


    public
    function actionUpdateCompanyContacts()
    {
        Loader::includeModule("crm");
        $start = microtime(true);
        $arContactParams = array(
            "Телефон" => array("TYPE" => "PHONE"),
            "Адрес электронной почты" => array("TYPE" => "EMAIL"),
        );
        //2. получим всех менеджеров, для привязки контактов по ГУИД
        $arUsers = $this->getBXUsers();

        $exit = false;
        $limit = 500;

        $cntAdd = 0;
        $cntUpdate = 0;
        $currentStep = 0;

        while (!$exit) {
            $currentStep++;
            //1. получим первые записи на апдейт из таблицы синхронизации
            if ($this->entity_data_class_contact) {
                $res = $this->entity_data_class_contact::getList(
                    [
                        "select" => ["ID", "UF_GUID", "UF_1C_RAW_DATA"],
                        'order' => ['ID' => 'ASC'],
                        'filter' => [
                            'LOGIC' => 'OR',
                            "UF_PROCESS_FLAG" => "U",
                            //считаем, что если записи висят в обработке дольше 10 минут, то предъидущий процесс упал и не доделал дело
                            //захватываем их на борт лайнера "обработка" в этот проход
                            [
                                'LOGIC' => 'AND',
                                'UF_PROCESS_FLAG' => "P",
                                "<=UF_TIMESTAMP" => \Bitrix\Main\Type\DateTime::createFromTimestamp(strtotime("-3 min"))
                            ]
                        ],
                        'limit' => $limit
                    ]
                );

                $dataCollection = $res->fetchCollection();

                if ($dataCollection->isEmpty()) {
                    $exit = true;
                    break;
                }

                $arGUID = [];
                $arCompanyGUID = [];
                foreach ($dataCollection as $elementCollection) {
                    $ar = [];
                    $ar["ID"] = $elementCollection->get("ID");
                    $ar["UF_GUID"] = $elementCollection->get("UF_GUID");
                    $ar["UF_1C_RAW_DATA"] = unserialize($elementCollection->get("UF_1C_RAW_DATA"));

                    $arGUID[$ar["UF_1C_RAW_DATA"]["СсылкаGUID"]] = $ar;

                    if ($ar["UF_1C_RAW_DATA"]["ПартнерGUID"]) {
                        $arCompanyGUID[$ar["UF_1C_RAW_DATA"]["ПартнерGUID"]] = $ar["UF_1C_RAW_DATA"]["ПартнерGUID"];
                    }

                    $elementCollection->set("UF_PROCESS_FLAG", "P");
                    $elementCollection->set("UF_TIMESTAMP", \Bitrix\Main\Type\DateTime::createFromTimestamp(time()));
                }
                //устанаввливаем флаги и время апдейта
                $dataCollection->save(true);

                // проверим наличие компаний в CRM
                $dbFields = CCrmCompany::GetList(
                    [],
                    [
                        "UF_GUID" => array_keys($arCompanyGUID),
                        ["ID"],
                        false
                    ]
                );

                while ($arResultCompany = $dbFields->Fetch()) {
                    $arCompanyID[$arResultCompany["ORIGIN_ID"]] = $arResultCompany["ID"];
                }

                //перебираем все контакты
                foreach ($arGUID as $arCont) {
                    $arContact = $arCont["UF_1C_RAW_DATA"];

                    $arNewContact = false;

                    // данные контакта
                    $arNewContact = array(
                        "ORIGIN_ID" => $arContact["СсылкаGUID"],
                        "UF_GUID" => $arContact["СсылкаGUID"],
                        "ORIGINATOR_ID" => "1C",
                        "SOURCE_ID" => "1C",
                        "NAME" => $arContact["Наименование"],
                        "COMPANY_TYPE" => "CUSTOMER",
                        "ASSIGNED_BY_ID" => isset($arUsers[$arContact["АвторGUID"]]) ? $arUsers[$arContact["АвторGUID"]] : 1,
                        "POST" => $arContact["ДолжностьПоВизитке"],
                        "OPENED" => "Y",
                        "COMPANY_IDS" => isset($arCompanyID[$arContact["ПартнерGUID"]]) ? [$arCompanyID[$arContact["ПартнерGUID"]]] : "", // привязка к компании
                    );

                    // обрабатываем контакнтую информацию
                    $nKeyContact = 0;
                    foreach ($arContact["ТабличныеЧасти"][0]["КонтактнаяИнформация"] as $contacts) {
                        if ($contacts["Тип"] == "Адрес") {
                            $addressType = "ADDRESS";
                            if ($contacts["Вид"] == "Юридический адрес") {
                                $addressType = "REG_ADDRESS";
                            }
                            $arNewContact[$addressType] = $contacts["Представление"];
                        } else {
                            $arNewContact["FM"][$arContactParams[$contacts["Тип"]]["TYPE"]] = array(
                                "n" . $nKeyContact++ => array(
                                    "VALUE_TYPE" => "WORK",
                                    "VALUE" => $contacts["Представление"],
                                )
                            );
                        }
                    }

                    $contact = new CCrmContact(false);

                    //проверка на существование контакта в БУС
                    $contactID = Crm\ContactTable::query()
                            ->setFilter(["ORIGIN_ID" => $arContact["СсылкаGUID"]])
                            ->addSelect("ID")
                            ->addSelect("ORIGIN_ID")
                            ->setLimit(1)
                            ->setCacheTtl(60)
                            ->exec()
                            ->fetch()["ID"]
                        ?? false;

                    //если контакта нет - добавляем
                    if (!$contactID) {
                        $contactID = $contact->Add($arNewContact);
                        $cntAdd++;
                    } //если есть - сохраняем
                    else {
                        $contact->Update($contactID, $arNewContact);
                        $cntUpdate++;
                    }
                    $this->entity_data_class_contact::update(
                        $arCont["ID"],
                        [
                            "UF_PROCESS_FLAG" => "",
                            "UF_TIMESTAMP" => \Bitrix\Main\Type\DateTime::createFromTimestamp(time())
                        ]
                    );
                }
            }

            $cntAllUpdate = $this->checkUpdateContacts();
            $arResult["LOG"] = "Шаг $currentStep. Всего обновили $cntUpdate компаний, добавили новых $cntAdd за " . round(microtime(true) - $start, 4) . " секунд. Осталось обновить " . $cntAllUpdate . ". Займет примерно " . round($cntAllUpdate / $limit) . " шагов.";

            COption::SetOptionString("main", "cntUpdateContacts", $arResult["LOG"]);
        }
        $cntAllUpdate = $this->checkUpdateContacts();
        COption::SetOptionString("main", "cntUpdateContacts", "Закончили перенос контактов из временной таблицы за $currentStep шагов. Всего на апдейт стоит $cntAllUpdate. Времени заняло " . round(microtime(true) - $start, 4) . " секунд.");
    }
}


?>
