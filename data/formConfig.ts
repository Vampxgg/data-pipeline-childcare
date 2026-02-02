import type { FormField } from '@/components/DynamicForm.vue';

// 第一部分：机构信息（园长填写）
export const step1Fields: FormField[] = [
  {
    type: 'remote-select',
    label: '机构名称',
    key: 'orgName',
    placeholder: '请输入关键字搜索机构',
    span: 2,
    rules: [{ required: true, message: '请选择所属机构' }],
    remoteConfig: {
      apiUrl: '/api-tuoyu/bapfopm/pub/search/action/queryInfo',
      searchKey: 'key',
      resultMap: { label: 'institution_name', value: 'institution_name' },
      format: (item: any) => {
        const parts = [];
        // 机构类型
        if (item.institution_type) parts.push(`类型：${item.institution_type}`);
        // 曾用名/其他名
        if (item.institution_other_name && item.institution_other_name !== item.institution_name) {
          parts.push(`曾用名：${item.institution_other_name}`);
        }
        // 行政区划
        if (item.zoning_name) parts.push(`区域：${item.zoning_name}`);
        // 地址
        if (item.address) parts.push(`地址：${item.address}`);
        // 建成时间
        if (item.finished_time) parts.push(`建成时间：${item.finished_time}`);

        return parts.join('\n');
      }
    }
  },
  {
    type: 'input',
    label: '园所/机构所在地',
    key: 'location',
    placeholder: '省-市-区',
    rules: [{ required: true, message: '请输入所在地' }],
  },
  {
    type: 'radio',
    label: '机构举办主体',
    key: 'orgNature',
    options: [
      { label: '公办', value: 'public' },
      { label: '民办', value: 'private' },
      { label: '公办民营/民办公助', value: 'inclusive_private' },
    ],
    rules: [{ required: true, message: '请选择机构性质' }],
  },
  {
    type: 'select',
    label: '机构具体形态',
    key: 'orgType',
    placeholder: '请选择机构类型',
    rules: [{ required: true, message: '请选择机构类型' }],
    options: [
      { label: '独立托育机构', value: '独立托育机构' },
      { label: '社区嵌入式托育', value: '社区嵌入式托育' },
      { label: '幼儿园托班', value: '幼儿园托班' },
      { label: '用人单位办托', value: '用人单位办托' },
      { label: '家庭托育点', value: '家庭托育点' },
      { label: '托育综合服务中心', value: '托育综合服务中心' },
    ],
  },
  {
    type: 'radio',
    label: '是否为普惠托育',
    key: 'isPovertyFree',
    options: [
      { label: '是', value: 'yes' },
      { label: '否', value: 'no' },
    ],
    rules: [{ required: true, message: '请选择是否为普惠托育' }],
  },
  {
    type: 'select',
    label: '机构服务模式',
    key: 'serviceMode',
    placeholder: '请选择机构服务模式',
    props: { mode: 'multiple' },
    rules: [{ required: true, message: '请选择机构服务模式' }],
    options: [
      { label: '全日托', value: '全日托' },
      { label: '半日托', value: '半日托' },
      { label: '计时托', value: '计时托' },
      { label: '临时托', value: '临时托' },
    ],
  },
  {
    type: 'input',
    label: '机构托位总数',
    key: 'totalSlots',
    props: { suffix: '个', type: 'number' },
    rules: [{ required: true, message: '请输入机构托位总数' }],
  },
  {
    type: 'input',
    label: '机构在园婴幼儿总数',
    key: 'totalChildren',
    props: { suffix: '人', type: 'number' },
    rules: [{ required: true, message: '请输入机构在园婴幼儿总数' }],
  },
  {
    type: 'input',
    label: '机构员工总人数',
    key: 'totalStaff',
    props: { suffix: '人', type: 'number' },
    rules: [{ required: true, message: '请输入机构员工总人数' }],
  },
];

// 第二部分：个人信息
export const step2Fields: FormField[] = [
  {
    type: 'radio',
    label: '性别',
    key: 'gender',
    options: [
      { label: '男', value: 'male' },
      { label: '女', value: 'female' },
    ],
    rules: [{ required: true, message: '请选择性别' }],
  },
  {
    type: 'select',
    label: '最高学历',
    key: 'education',
    placeholder: '请选择最高学历',
    options: [
      { label: '高中/中职', value: 'senior_high_school' },
      { label: '高职专科', value: 'vocational_college' },
      { label: '高职本科', value: 'vocational_university' },
      { label: '普通本科', value: 'undergraduate' },
      { label: '硕士研究生', value: 'master_degree' },
    ],
    rules: [{ required: true, message: '请选择最高学历' }],
  },
  {
    type: 'input',
    label: '最高学历所学专业',
    key: 'educationMajor',
    rules: [{ required: true, message: '请输入最高学历所学专业' }],
  },
];

// 第三部分：从业信息
export const step3Fields: FormField[] = [
  {
    type: 'select',
    label: '当前岗位',
    key: 'currentPosition',
    options: [
      { label: '园长/负责人', value: 'director' },
      { label: '主班教师', value: 'main_teacher' },
      { label: '配班教师', value: 'support_teacher' },
      { label: '保育员', value: 'caregiver' },
      { label: '保健医', value: 'doctor' },
      { label: '其他', value: 'other' },
    ],
    rules: [{ required: true, message: '请选择当前岗位' }],
  },
  {
    type: 'select',
    label: '从业以来更换托育机构的时间间隔',
    key: 'interval',
    placeholder: '请选择从业以来更换托育机构的时间间隔',
    options: [
      { label: '1年以内', value: '1' },
      { label: '1-3年', value: '1-3' },
      { label: '3-5年', value: '3-5' },
      { label: '5年以上', value: '5+' },
      { label: '从未更换', value: 'never' },
    ],
    rules: [{ required: true, message: '请选择从业以来更换托育机构的时间间隔' }],
  },
  {
    type: 'select',
    label: '从业以来更换托育机构的原因',
    key: 'reason',
    placeholder: '请选择从业以来更换托育机构的原因',
    props: { mode: 'multiple' },
    options: [
      { label: '个人原因', value: 'personal' },
      { label: '家庭原因', value: 'family' },
      { label: '机构原因', value: 'institution' },
      { label: '其他', value: 'other' },
    ],
    rules: [{ required: true, message: '请选择从业以来更换托育机构的原因' }],
  },
  {
    type: 'select',
    label: '当前薪资范围',
    key: 'salaryRange',
    placeholder: '请选择当前薪资范围',
    options: [
      { label: '1000-3000元', value: '1000-2000' },
      { label: '3000-5000元', value: '3000-5000' },
      { label: '5000-8000元', value: '5000-8000' },
      { label: '8000-10000元', value: '8000-10000' },
      { label: '10000元以上', value: '10000+' },
    ],
  },
  {
    type: 'radio',
    label: '是否从幼儿园班级教师转型成为托班教师',
    key: 'isFromTeacherToTeacher',
    options: [
      { label: '是', value: 'yes' },
      { label: '否', value: 'no' },
    ],
    rules: [{ required: true, message: '请选择是否从幼儿园班级教师转型成为托班教师' }],
  },
  {
    type: 'textarea',
    label: '从幼儿园班级教师转型成为托班教师的原因',
    key: 'reasonFromTeacherToTeacher',
    rules: [{ required: true, message: '请输入从幼儿园班级教师转型成为托班教师的原因' }],
  },
];

// 第四部分：岗位相关信息（示例：根据不同岗位返回不同配置）
export const getStep4Fields = (position: string): FormField[] => {
  const commonFields: FormField[] = [
    {
      type: 'textarea',
      label: '工作主要职责',
      key: 'responsibilities',
      rules: [{ required: true, message: '请填写主要职责' }],
    }
  ];

  if (position === 'director') {
    return [
      ...commonFields,
      {
        type: 'checkbox',
        label: '您日常工作最核心的任务是',
        key: 'coreTasks',
        span: 2,
        props: { mode: 'multiple' },  
        options: [
          { label: '年度工作计划制定与定期报告', value: 'annual_plan' },
          { label: '托育机构场地、建筑设计、室内外环境、设施设备、图书与游戏材料等规范的设置', value: 'setting_up_the_institution' },
          { label: '信息管理、健康管理、膳食管理、疾病防控、安全防护、人员管理、人员培训、财务管理、家长与社区联系等制度的建立与实施', value: 'Implementation' },
          { label: '教职工团队建设、培训与绩效管理', value: 'team_building_and_training' },
          { label: '一日生活安排与指导', value: 'daily_life_arrangement_and_guidance' },
          { label: '动作、语言、认知、情感与社会性等保育活动组织与指导', value: 'physical_language_cognitive_emotional_and_social_development_guidance' },
          { label: '环境创设', value: 'environment_creation' },
          { label: '照护服务日常记录和反馈', value: 'daily_record_and_feedback' },
          { label: '保育人员工作的检查和评估', value: 'inspection_and_evaluation_of_caregivers' },
          { label: '招生运营、品牌建设', value: 'marketing_and_brand_building' },
          { label: '对外合作与资源整合', value: 'external_collaboration_and_resource_integration' },
        ],
      },
    ];
  } else if (['main_teacher', 'support_teacher'].includes(position)) {
    return [
      ...commonFields,
      {
        type: 'radio',
        label: '最需要的专业技能培训',
        key: 'trainingNeeds',
        span: 2,
        options: [
          { label: '课程设计', value: 'curriculum' },
          { label: '家园沟通', value: 'communication' },
          { label: '儿童心理', value: 'psychology' },
        ],
      },
      {
        type: 'matrix',
        label: '请选择下列各项素质素养的重要程度',
        key: 'competency_matrix',
        span: 2,
        props: {
          rowTitle: '素质素养',
          columns: [
            { label: '非常不重要', value: 1 },
            { label: '不重要', value: 2 },
            { label: '一般', value: 3 },
            { label: '重要', value: 4 },
            { label: '非常重要', value: 5 },
          ],
          rows: [
            { label: '热爱本职，以德立身', value: 'item1' },
            { label: '负责奉献，主动承担，乐于分享', value: 'item2' },
            { label: '尊重婴幼儿个体差异、平等对待每一位幼儿的理念', value: 'item3' },
            { label: '严格遵守教师职业道德规范', value: 'item4' },
            { label: '善于自我情绪调节，保持平和心态与稳定情绪', value: 'item5' },
            { label: '言谈举止文明得体，仪表端庄大方', value: 'item6' },
            { label: '善于沟通协作，尊重他人、互相理解', value: 'item7' },
            { label: '自觉遵守幼儿园各项规章制度，坚守工作岗位、履职尽责', value: 'item8' },
            { label: '以热爱、尊重、平等的态度对待每一名幼儿', value: 'item9' },
            { label: '主动了解并满足幼儿身心发展的个性化需求，关注每名幼儿的成长进程', value: 'item10' },
            { label: '坚持正面教育与引导原则', value: 'item11' },
            { label: '遵守教育法律法规，严格执行机构保育与教育目标', value: 'item12' },
            { label: '灵活运用适宜的方法、手段与途径开展工作', value: 'item13' },
            { label: '持续学习、追求专业成长的进取意识', value: 'item14' },
          ]
        },
      },
    ];
  } else if (position === 'caregiver') {
    return [
      ...commonFields,
      {
        type: 'radio',
        label: '最需要的保育技能培训',
        key: 'careSkills',
        span: 2,
        options: [
          { label: '卫生消毒', value: 'hygiene' },
          { label: '意外伤害处理', value: 'accident' },
          { label: '营养喂养', value: 'nutrition' },
        ],
      },
    ];
  }

  return commonFields;
};

// 第五部分：园长/负责人专属
export const step5Fields: FormField[] = [
  {
    type: 'input',
    label: '机构总人数',
    key: 'totalStaff',
    rules: [{ required: true, message: '请输入总人数' }],
  },
  {
    type: 'checkbox',
    label: '未来三年人才需求',
    key: 'futureTalentNeeds',
    span: 2,
    options: [
      { label: '专业教师', value: 'teacher' },
      { label: '保育员', value: 'caregiver' },
      { label: '保健医', value: 'doctor' },
      { label: '管理人员', value: 'manager' },
    ],
  },
  {
    type: 'textarea',
    label: '对人才培养的建议',
    key: 'suggestions',
    span: 2,
  },
];
