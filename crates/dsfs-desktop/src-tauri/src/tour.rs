//! Guided tour definitions and state machine.

use serde::Serialize;

#[derive(Clone, Debug)]
pub struct TourStep {
    pub title: &'static str,
    pub body: &'static str,
    pub action: TourAction,
    pub wait_for: WaitCondition,
    pub highlights: &'static [Highlight],
    pub button_label: &'static str,
}

#[derive(Clone, Debug)]
pub enum TourAction {
    None,
    StartEngine,
    CrashLeader,
    CrashNode(u32),
    RestartNode(u32),
    Partition {
        group_a: &'static [u32],
        group_b: &'static [u32],
    },
    Heal {
        group_a: &'static [u32],
        group_b: &'static [u32],
    },
    SubmitWrite { key: &'static str, value: &'static str },
}

#[derive(Clone, Debug, Serialize)]
pub enum Highlight {
    Node(u32),
    ClusterPanel,
    EventsPanel,
    CheckerPanel,
    FaultBar,
}

#[derive(Clone, Debug)]
pub enum WaitCondition {
    Immediate,
    LeaderExists,
    NewLeaderAfterCrash,
    CandidateExists,
    FaultApplied(&'static str),
    AppliedCommand(&'static str),
    NodeOffline(u32),
    NodeOnline(u32),
    NodeAppliedCommand(u32, &'static str),
    TimeReached(u64),
}

/// Which scenario a tour uses.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize)]
pub enum TourKind {
    LeaderFailure,
    NetworkPartition,
    NodeRecovery,
}

impl TourKind {
    #[must_use]
    pub fn scenario_name(self) -> &'static str {
        match self {
            TourKind::LeaderFailure => "leader_failure",
            TourKind::NetworkPartition => "network_partition",
            TourKind::NodeRecovery => "node_recovery",
        }
    }

    #[must_use]
    pub fn steps(self) -> &'static [TourStep] {
        match self {
            TourKind::LeaderFailure => LEADER_FAILURE_TOUR,
            TourKind::NetworkPartition => NETWORK_PARTITION_TOUR,
            TourKind::NodeRecovery => NODE_RECOVERY_TOUR,
        }
    }
}

// ---- Tour 1: Leader Failure ----

pub const LEADER_FAILURE_TOUR: &[TourStep] = &[
    TourStep {
        title: "Лидер падает",
        body: "Кластер из 5 узлов. Один из них — лидер. Остальные — реплики: \
               они копируют данные лидера, чтобы если он упадёт, данные не потерялись.\n\n\
               Мы смоделируем реальный отказ: лидер внезапно упадёт. И \
               проверим, что система сохранит корректность.",
        action: TourAction::StartEngine,
        wait_for: WaitCondition::Immediate,
        highlights: &[Highlight::ClusterPanel],
        button_label: "Запустить",
    },
    TourStep {
        title: "Лидер избран",
        body: "Узел с меткой ★ — лидер. Он отправляет остальным узлам \
               heartbeat каждые 30 миллисекунд.\n\n\
               Пока лидер жив и шлёт heartbeat, остальные узлы спокойны: \
               они знают, что лидер работает.",
        action: TourAction::None,
        wait_for: WaitCondition::LeaderExists,
        highlights: &[Highlight::ClusterPanel],
        button_label: "Далее",
    },
    TourStep {
        title: "Клиент записывает данные",
        body: "Клиент хочет сохранить: balance = 500.\n\n\
               Запись отправляется лидеру. Лидер пишет её к себе в журнал, \
               потом реплицирует на других узлах.\n\n\
               Запись считается подтверждённой, только когда её получило \
               большинство узлов (3 из 5).",
        action: TourAction::SubmitWrite {
            key: "balance",
            value: "500",
        },
        wait_for: WaitCondition::AppliedCommand("balance"),
        highlights: &[Highlight::ClusterPanel, Highlight::EventsPanel],
        button_label: "Далее",
    },
    TourStep {
        title: "Лидер падает",
        body: "Мы отключаем лидера.\n\n\
               В реальной системе это могло быть падение процесса, сбой \
               питания или отказ сети. Система об этом не знает — она \
               просто перестаёт получать ответы.\n\n\
               Обрати внимание: часть узлов уже получила запись, часть — нет. \
               Что теперь?",
        action: TourAction::CrashLeader,
        wait_for: WaitCondition::FaultApplied("crash"),
        highlights: &[Highlight::ClusterPanel, Highlight::EventsPanel],
        button_label: "Смотреть",
    },
    TourStep {
        title: "Остальные замечают проблему",
        body: "Узлы перестают получать heartbeat от лидера.\n\n\
               Через 150-300 миллисекунд у них срабатывает election timeout — \
               таймер, который заставляет узел задуматься: «А лидер жив?»\n\n\
               Один из узлов становится кандидатом и начинает сбор голосов.",
        action: TourAction::None,
        wait_for: WaitCondition::CandidateExists,
        highlights: &[Highlight::ClusterPanel, Highlight::EventsPanel],
        button_label: "Смотреть выборы",
    },
    TourStep {
        title: "Избран новый лидер",
        body: "Большинство (3 из 5 живых узлов) проголосовало за нового лидера. \
               Кластер продолжает работу.\n\n\
               Ключевой вопрос: что случилось с той записью, которую клиент \
               отправил прямо перед падением?",
        action: TourAction::None,
        wait_for: WaitCondition::NewLeaderAfterCrash,
        highlights: &[Highlight::ClusterPanel],
        button_label: "Проверить",
    },
    TourStep {
        title: "Проверка инвариантов",
        body: "Мы проверяем инвариант Raft: «No Lost Committed Entries».\n\n\
               Если запись была закоммичена — она должна быть в журнале у \
               нового лидера. Если нет — она могла быть потеряна (это \
               нормально: клиент не получил подтверждения).\n\n\
               Но она не может быть закоммичена на одном узле и потеряна на \
               другом. Это нарушило бы согласованность.",
        action: TourAction::None,
        wait_for: WaitCondition::Immediate,
        highlights: &[Highlight::EventsPanel],
        button_label: "Завершить",
    },
    TourStep {
        title: "Что мы доказали",
        body: "Мы воспроизвели реальный сценарий отказа, который в production \
               был бы редким и трудноуловимым.\n\n\
               Запись журнала сохранена с seed=42. Ты можешь воспроизвести \
               этот же сценарий в любой момент — результат будет идентичным.\n\n\
               Это и есть DSFS: контролируемое исследование поведения \
               распределённых систем при отказах.",
        action: TourAction::None,
        wait_for: WaitCondition::Immediate,
        highlights: &[Highlight::ClusterPanel, Highlight::EventsPanel],
        button_label: "Начать заново",
    },
];

// ---- Tour 2: Network Partition ----

pub const NETWORK_PARTITION_TOUR: &[TourStep] = &[
    TourStep {
        title: "Сетевое разделение",
        body: "Кластер из 5 узлов. Лидер отправляет heartbeat, остальные \
               реплицируют записи.\n\n\
               Теперь представим: сеть между узлами ломается. Кластер \
               распадается на две части: 2 узла в одной, 3 в другой.\n\n\
               Вопрос: какая часть продолжит работу?",
        action: TourAction::StartEngine,
        wait_for: WaitCondition::Immediate,
        highlights: &[Highlight::ClusterPanel],
        button_label: "Запустить",
    },
    TourStep {
        title: "Лидер избран",
        body: "Кластер работает нормально. Узел ★ — лидер, остальные его \
               слушают.\n\n\
               Всё стабильно. Сейчас мы «сломаем сеть».",
        action: TourAction::None,
        wait_for: WaitCondition::LeaderExists,
        highlights: &[Highlight::ClusterPanel],
        button_label: "Далее",
    },
    TourStep {
        title: "Разделяем сеть на 2 + 3",
        body: "Мы разрываем все связи между группой {n1, n2} и группой \
               {n3, n4, n5}.\n\n\
               В реальности это могло быть: обрыв кабеля между стойками, \
               сбой маршрутизатора, порезанный VPN.\n\n\
               Узлы не могут общаться между группами. Но внутри группы — могут.",
        action: TourAction::Partition {
            group_a: &[1, 2],
            group_b: &[3, 4, 5],
        },
        wait_for: WaitCondition::FaultApplied("partition"),
        highlights: &[Highlight::ClusterPanel, Highlight::EventsPanel],
        button_label: "Смотреть",
    },
    TourStep {
        title: "Меньшая группа застревает",
        body: "В группе {n1, n2} — только 2 узла. Это меньше большинства \
               (нужно 3 из 5).\n\n\
               Если лидер был в этой группе, он шлёт heartbeat соседям, но \
               не может получить подтверждение записи от большинства. Он \
               пытается, но commit не наступает.\n\n\
               Меньшая группа — заблокирована. Она не может принимать изменения.",
        action: TourAction::None,
        wait_for: WaitCondition::Immediate,
        highlights: &[Highlight::ClusterPanel, Highlight::EventsPanel],
        button_label: "А что в большей?",
    },
    TourStep {
        title: "Большая группа работает",
        body: "В группе {n3, n4, n5} — 3 узла. Это большинство.\n\n\
               Если здесь есть лидер — он продолжает принимать записи. Если \
               нет — узлы проводят выборы и избирают нового лидера внутри \
               группы.\n\n\
               Split-brain невозможен: только одна группа может иметь лидера, \
               потому что лидер требует голосов большинства.",
        action: TourAction::None,
        wait_for: WaitCondition::Immediate,
        highlights: &[Highlight::ClusterPanel],
        button_label: "Проверить консистентность",
    },
    TourStep {
        title: "Split-brain невозможен",
        body: "В Raft узел становится лидером, только если за него проголосовало \
               большинство.\n\n\
               В группе {n1, n2} максимум 2 голоса — этого недостаточно. Даже \
               если оба узла станут кандидатами, они не наберут большинства.\n\n\
               Поэтому в кластере в любой момент времени — не более одного \
               лидера. Это фундаментальное свойство.",
        action: TourAction::None,
        wait_for: WaitCondition::Immediate,
        highlights: &[Highlight::EventsPanel],
        button_label: "Восстановить сеть",
    },
    TourStep {
        title: "Восстанавливаем сеть",
        body: "Мы «лечим» сеть. Узлы снова видят друг друга.\n\n\
               Меньшая группа {n1, n2} обнаруживает, что в большей группе был \
               избран новый лидер (с более высоким term).\n\n\
               Они отказываются от своей роли и синхронизируют журнал с новым \
               лидером.",
        action: TourAction::Heal {
            group_a: &[1, 2],
            group_b: &[3, 4, 5],
        },
        wait_for: WaitCondition::FaultApplied("heal"),
        highlights: &[Highlight::ClusterPanel, Highlight::EventsPanel],
        button_label: "Далее",
    },
    TourStep {
        title: "Что мы доказали",
        body: "Мы воспроизвели split-brain сценарий — один из самых опасных \
               отказов в распределённых системах.\n\n\
               В не-консенсусных системах (например, асинхронная репликация) \
               обе части разделённой сети могут независимо принимать изменения. \
               Это приводит к конфликтам и потере данных при восстановлении.\n\n\
               Raft этого не допускает: лидер требует большинства. Меньшая \
               группа блокируется, пока сеть не восстановится.",
        action: TourAction::None,
        wait_for: WaitCondition::Immediate,
        highlights: &[Highlight::ClusterPanel, Highlight::EventsPanel],
        button_label: "Начать заново",
    },
];

// ---- Tour 3: Node Recovery ----

pub const NODE_RECOVERY_TOUR: &[TourStep] = &[
    TourStep {
        title: "Восстановление узла",
        body: "Кластер из 5 узлов. Лидер реплицирует записи на всех.\n\n\
               Что произойдёт, если один из узлов выключится на несколько секунд, \
               а потом вернётся?\n\n\
               Его журнал будет отставать. Raft должен синхронизировать его — \
               без потери консистентности.",
        action: TourAction::StartEngine,
        wait_for: WaitCondition::Immediate,
        highlights: &[Highlight::ClusterPanel],
        button_label: "Запустить",
    },
    TourStep {
        title: "Лидер избран, кластер работает",
        body: "Узел ★ — лидер. Все узлы синхронизированы.\n\n\
               Сейчас мы отключим один узел (n4).",
        action: TourAction::None,
        wait_for: WaitCondition::LeaderExists,
        highlights: &[Highlight::ClusterPanel],
        button_label: "Далее",
    },
    TourStep {
        title: "Клиент пишет запись",
        body: "Клиент пишет: balance = 500.\n\n\
               Запись уходит лидеру, реплицируется на большинство узлов, \
               применяется.",
        action: TourAction::SubmitWrite {
            key: "balance",
            value: "500",
        },
        wait_for: WaitCondition::AppliedCommand("balance"),
        highlights: &[Highlight::ClusterPanel, Highlight::EventsPanel],
        button_label: "Далее",
    },
    TourStep {
        title: "Узел n4 отключается",
        body: "Мы отключаем n4.\n\n\
               Для системы это выглядит как зависший процесс или отказ сети — \
               n4 не отвечает и не получает сообщений.\n\n\
               Остальные 4 узла продолжают работу: 4 из 5 — это большинство.",
        action: TourAction::CrashNode(4),
        wait_for: WaitCondition::FaultApplied("crash"),
        highlights: &[Highlight::ClusterPanel, Highlight::EventsPanel],
        button_label: "Проверить n4",
    },
    TourStep {
        title: "n4 отстал от кластера",
        body: "Смотри на журнал n4: term и log остановились в момент отключения. \
               Остальные узлы продолжают реплицировать записи.\n\n\
               n4 сейчас — «замороженный снимок» кластера на момент отключения.",
        action: TourAction::None,
        wait_for: WaitCondition::NodeOffline(4),
        highlights: &[Highlight::ClusterPanel],
        button_label: "Записать ещё",
    },
    TourStep {
        title: "Клиент пишет ещё запись",
        body: "Клиент пишет: balance = 1000.\n\n\
               Лидер принимает запись. Реплицирует на большинство.\n\n\
               Но n4 — offline. Он не получает эту запись. Она есть на 4 узлах \
               из 5.",
        action: TourAction::SubmitWrite {
            key: "balance",
            value: "1000",
        },
        wait_for: WaitCondition::AppliedCommand("1000"),
        highlights: &[Highlight::ClusterPanel, Highlight::EventsPanel],
        button_label: "Проверить n4",
    },
    TourStep {
        title: "Журнал n4 отстал",
        body: "Смотри на log n4: он не получил запись balance=1000. Его commit \
               index меньше, чем у остальных.\n\n\
               Это нормально: n4 был offline в момент репликации.\n\n\
               Теперь вернём его в кластер.",
        action: TourAction::None,
        wait_for: WaitCondition::Immediate,
        highlights: &[Highlight::ClusterPanel],
        button_label: "Вернуть n4",
    },
    TourStep {
        title: "Возвращаем n4 в кластер",
        body: "Мы включаем n4 обратно.\n\n\
               При первом же heartbeat от лидера n4 понимает, что его журнал \
               отстаёт. Лидер начинает отправлять ему недостающие записи.",
        action: TourAction::RestartNode(4),
        wait_for: WaitCondition::FaultApplied("restart"),
        highlights: &[Highlight::ClusterPanel, Highlight::EventsPanel],
        button_label: "Смотреть синхронизацию",
    },
    TourStep {
        title: "n4 догоняет журнал",
        body: "Лидер отправляет n4 AppendEntries с недостающими записями.\n\n\
               n4 принимает их, обновляет свой журнал, применяет \
               balance=1000.\n\n\
               Через несколько heartbeat n4 полностью синхронизирован с \
               кластером.",
        action: TourAction::None,
        wait_for: WaitCondition::NodeAppliedCommand(4, "1000"),
        highlights: &[Highlight::ClusterPanel, Highlight::EventsPanel],
        button_label: "Проверить",
    },
    TourStep {
        title: "Что мы доказали",
        body: "n4 отсутствовал в момент записи, но после возврата догнал журнал \
               и теперь имеет то же состояние, что и остальные.\n\n\
               Ключевое свойство Raft: узел, вернувшийся после отказа, \
               автоматически синхронизируется с лидером. Ничего не теряется \
               и не дублируется.\n\n\
               В production это позволяет переживать падения отдельных узлов \
               без ручного вмешательства.",
        action: TourAction::None,
        wait_for: WaitCondition::Immediate,
        highlights: &[Highlight::ClusterPanel, Highlight::EventsPanel],
        button_label: "Начать заново",
    },
];

/// Runtime state.
#[derive(Clone, Debug, Default, Serialize)]
pub struct TourState {
    pub active: bool,
    pub finished: bool,
    pub current_step: usize,
    pub total_steps: usize,
    pub step_title: String,
    pub step_body: String,
    pub step_button_label: String,
    pub highlights: Vec<Highlight>,
    pub step_ready: bool,
    pub kind: Option<TourKind>,
}

impl TourState {
    #[must_use]
    pub fn inactive() -> Self {
        Self {
            active: false,
            finished: false,
            current_step: 0,
            total_steps: 0,
            step_title: String::new(),
            step_body: String::new(),
            step_button_label: String::new(),
            highlights: Vec::new(),
            step_ready: false,
            kind: None,
        }
    }

    pub fn start(&mut self, kind: TourKind) {
        self.kind = Some(kind);
        self.active = true;
        self.finished = false;
        self.current_step = 0;
        self.total_steps = kind.steps().len();
        self.load_current_step();
    }

    pub fn advance(&mut self) {
        if !self.active {
            return;
        }
        let Some(kind) = self.kind else { return };
        let steps = kind.steps();
        if self.current_step + 1 >= steps.len() {
            self.finished = true;
            self.active = false;
            return;
        }
        self.current_step += 1;
        self.load_current_step();
    }

    fn load_current_step(&mut self) {
        let Some(kind) = self.kind else { return };
        if let Some(step) = kind.steps().get(self.current_step) {
            self.step_title = step.title.to_string();
            self.step_body = step.body.to_string();
            self.step_button_label = step.button_label.to_string();
            self.highlights = step.highlights.to_vec();
            self.step_ready = matches!(step.wait_for, WaitCondition::Immediate);
        }
    }

    #[must_use]
    pub fn current_wait_condition(&self) -> Option<&WaitCondition> {
        let kind = self.kind?;
        kind.steps().get(self.current_step).map(|s| &s.wait_for)
    }

    #[must_use]
    pub fn current_action(&self) -> Option<&TourAction> {
        let kind = self.kind?;
        kind.steps().get(self.current_step).map(|s| &s.action)
    }
}