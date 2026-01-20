create table wiki_item (
    id int(11) not null,
    title varchar(200) not null,
    dump_name varchar(100) not null,
    fulltext key title (title)
) engine=MyISAM default charset=utf8mb4 collate=utf8mb4_unicode_ci;

create table wiki_category (
    id int(11) not null,
    category varchar(100) default null,
    fulltext key category (category)
) engine=MyISAM default charset=utf8mb4 collate=utf8mb4_unicode_ci;

create table wiki_redirect (
    id int(11) not null,
    title varchar(200) default null,
    redirect varchar(200) default null,
    dump_name varchar(100) not null,
    fulltext key redirect (redirect)
) engine=MyISAM default charset=utf8mb4 collate=utf8mb4_unicode_ci;

create table wiki_seealso (
    id int(11) not null,
    seealso varchar(100) default null,
    fulltext key seealso (seealso)
) engine=MyISAM default charset=utf8mb4 collate=utf8mb4_unicode_ci;

create table wiki_section (
    id int(11) not null,
    section1 varchar(250) default null,
    section2 varchar(250) default null,
    section_text longtext default null,
    fulltext key section1 (section1)
) engine=MyISAM default charset=utf8mb4 collate=utf8mb4_unicode_ci;

create table check_seeds (
    no int not null auto_increment,
    title varchar(1000) default null,
    id int default null,
    true_title varchar(1000) default null,
    load_date datetime default current_timestamp,
    primary key (no),
    unique key uq_title (title)
) engine=InnoDB default charset=utf8mb4 collate=utf8mb4_unicode_ci;

create table seealso_expand (
    id int(11) not null,
    from_id int(11) default null,
    from_title varchar(1000) default null,
    to_id int(11) default null,
    to_title varchar(1000) default null,
    n_cnt int(11) default null
) engine=MyISAM default charset=utf8mb4 collate=utf8mb4_unicode_ci;

create table seealso_filter (
    id int(11) not null,
    from_id int(11) default null,
    from_title varchar(1000) default null,
    to_id int(11) default null,
    to_title varchar(1000) default null,
    n_cnt int(11) default null
) engine=MyISAM default charset=utf8mb4 collate=utf8mb4_unicode_ci;

create table item_master_temp (
    id int(11) not null,
    title varchar(1000) default null
) engine=MyISAM default charset=utf8mb4 collate=utf8mb4_unicode_ci;

create table item_master (
    id int(11) not null,
    title varchar(1000) default null
) engine=MyISAM default charset=utf8mb4 collate=utf8mb4_unicode_ci;

create table wiki_item_info (
    id int(11) not null,
    title varchar(200) default null,
    section_text longtext default null,
    translated longtext default null,
    category varchar(100) default null,
    sub_category varchar(100) default null,
    explanation longtext default null,
    tech_class12 varchar(200) default null,
    tech_class_reason longtext default null,
    primary key (id)
) engine=MyISAM default charset=utf8mb4 collate=utf8mb4_unicode_ci;

create table wikiitem_edit (
    id int(11) not null,
    item_name varchar(1000) not null,
    edits int(11) default null,
    year varchar(10) not null,
    crawl_date datetime default current_timestamp(),
    primary key (id,year)
) engine=MyISAM default charset=utf8mb4 collate=utf8mb4_unicode_ci;

create table crawl_xtool_pageview (
    id int(11) not null,
    project varchar(50) default null,
    article varchar(500) default null,
    granularity varchar(50) default null,
    reg_date varchar(50) not null,
    access varchar(50) default null,
    agent varchar(50) default null,
    views int(11) default null,
    crawl_date datetime default current_timestamp(),
    primary key (id,reg_date)
) engine=InnoDB default charset=utf8mb4 collate=utf8mb4_unicode_ci;

create table network_pagerank (
    id int(11) not null,
    pagerank decimal(30,25) default null
) engine=MyISAM default charset=utf8mb4 collate=utf8mb4_unicode_ci;

create table item_statistics (
    id int not null,
    base_year varchar(5) character set utf8mb4 collate utf8mb4_unicode_ci default null,
    pageviews decimal(20,10) default null,
    epv decimal(20,10) default null,
    norm_pageviews decimal(24,18) default null,
    norm_epv decimal(24,18) default null
) engine=MyISAM default charset=utf8mb4 collate=utf8mb4_unicode_ci;

create table wiki_itemlist_tb (
    id int not null,
    title varchar(300) collate utf8mb4_unicode_ci default null,
    pagerank double default null,
    pageviews double default null,
    epv double default null,
    key idx_id (id)
) engine=MyISAM default charset=utf8mb4 collate=utf8mb4_unicode_ci;

create table wiki_search_tb (
    id int not null,
    title varchar(300) character set utf8mb4 collate utf8mb4_unicode_ci default null,
    redirect varchar(300) character set utf8mb4 collate utf8mb4_unicode_ci default null,
    redirect_type varchar(10) character set utf8mb4 collate utf8mb4_unicode_ci default null,
    tech_rank int default null,
    tech_cnt int default null
) engine=MyISAM default charset=utf8mb4 collate=utf8mb4_unicode_ci;