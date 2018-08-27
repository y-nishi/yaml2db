require 'test/unit'
require 'yaml'
require_relative 'yaml2db'
require 'pp'

class TestSql_Column < Test::Unit::TestCase
  include Sql

  EMPTY_COLUMN_YAML = "error: error"
  PROPER_COLUMN_YAML = "{size: 100, default: 10, nullable: true, check: ['>= 100'], pname: PROP}"

  def setup
    @empty_data = Column.new(YAML.load(EMPTY_COLUMN_YAML))
    @proper_data = Column.new(YAML.load(PROPER_COLUMN_YAML))
  end

  def test_get_size
    assert_equal "", get_size(@empty_data)
    assert_equal "(100)", get_size(@proper_data)
  end

  def test_get_default
    assert_equal "", get_default(@empty_data)
    assert_equal "DEFAULT 10", get_default(@proper_data)
  end

  def test_get_null
    assert_equal "", get_null(@empty_data)
    assert_equal "", get_null(@proper_data)
  end
  
  def test_get_check
    assert_equal "", get_check(@empty_data)
    assert_equal " CHECK(PROP >= 100)", get_check(@proper_data)

    proper_data2 = Column.new(YAML.load("{pname: PROP, check: ['>= 100', '<= 1000']}"))
    assert_equal " CHECK(PROP >= 100) CHECK(PROP <= 1000)", get_check(proper_data2)

  end

end

class TestSql_Columns < Test::Unit::TestCase
  include Sql, YAMLLoadable

  def setup
    @domains = Domains.new(
      YAML.load(
        "domains: {domain1: {type: number, size: 10, default: 200, nullable: false}}"))
    @dict = Dictionary.new(
      YAML.load(
        "{dictionary: {word1: AAA, word2: BBB, word3: CCC}}"
      )
    )
  end

  def load_columns(parsetree, dict = nil, domains = nil)
    @columns = extract_element(parsetree, 'columns').map do |e|
      Column.new(e, dict, domains)
    end
  end

  def test_no_column
    assert_equal nil, extract_element(YAML.load("foo: bar"), 'columns', false)
    assert_raise(RuntimeError){extract_element(YAML.load("foo: bar"), 'columns')}
    assert_raise(RuntimeError){extract_element(YAML.load("[1,2,3]"), 'columns', false)}
  end

  def test_one_column
    load_columns YAML.load("columns: [{pname: TEST1, type: number, size: 4, default: 100, nullable: true, check: ['>= 100', 'nvl(<name>, 0) <= 1000']}]")
    assert_equal "TEST1 number(4) DEFAULT 100 CHECK(TEST1 >= 100) CHECK(nvl(TEST1, 0) <= 1000)", get_columns_def(@columns)
  end

  def test_columns
    load_columns YAML.load("columns: [{pname: TEST1, type: date, default: sysdate, nullable: false}, {pname: TEST2, type: varchar2, default: '''SP''', check: ['>= ''100''']}]")
    assert_equal "TEST1 date DEFAULT sysdate NOT NULL,\nTEST2 varchar2 DEFAULT 'SP' CHECK(TEST2 >= '100')", get_columns_def(@columns)
  end

  def test_one_column_domain
    load_columns YAML.load("columns: [{pname: TEST1, domain: domain1, check: '<name> >= 100'}]"), nil, @domains
    assert_equal "TEST1 number(10) DEFAULT 200 NOT NULL CHECK(TEST1 >= 100)", get_columns_def(@columns)

  end

  def test_domain_name_column
    load_columns YAML.load("columns: [{pname: TEST1, name: domain1}]"), nil, @domains
    assert_equal "TEST1 number(10) DEFAULT 200 NOT NULL", get_columns_def(@columns)
  end

  def test_make_pname_with_dict
    load_columns YAML.load("{columns: [{name: word1, type: number, nullable: true}]}"), @dict
    assert_equal "AAA number ", get_columns_def(@columns)

    load_columns YAML.load("{columns: [{name: word1.word2, type: number, nullable: true}]}"), @dict
    assert_equal "AAABBB number ", get_columns_def(@columns)

    load_columns YAML.load("{columns: [{pname: word1.word2, type: number, nullable: true}]}"), @dict
    assert_equal "word1.word2 number ", get_columns_def(@columns)
  end

  def test_indexes
    load_columns YAML.load(
      "{columns: [{name: word1, type: number, nullable: true, index2: 2, index1: 1}, {name: word2, type: number, index2: 1}, {name: word3, type: number}]}"
    ), @dict
    assert_equal 2, @columns[0].indexes["index2"]
    assert_equal 1, @columns[0].indexes["index1"]
    assert_equal 1, @columns[1].indexes["index2"]
  end

  def test_refer
    load_columns YAML.load("{columns: [{pname: TEST1, refers: {table: 1, order: 2, column: TEST2}},
    {pname: TEST3, refers: [{table: 0}, {table: 1}] } ]}")
    assert_equal 1, @columns[0].refers[0].table_index
    assert_equal 2, @columns[0].refers[0].order
    assert_equal "TEST2", @columns[0].refers[0].rcolumn
    assert_equal "TEST1", @columns[0].refers[0].column

    assert_equal 0, @columns[1].refers[0].table_index
    assert_equal 0, @columns[1].refers[0].order
    assert_equal "TEST3", @columns[1].refers[0].rcolumn
    assert_equal "TEST3", @columns[1].refers[1].rcolumn
  end

end

class TestTable < Test::Unit::TestCase
  include Sql, YAMLLoadable

  TABLE_SAMPLE = "{table: {name: testname, pname: test_table, columns: [{ name: word1.word2, domain: domain1}]}}"
  TABLE_SAMPLE_NOPNAME = "{table: {name: testname, columns: [{ name: word1.word2, domain: domain1}]}}"
  TABLE_SAMPLE_COLUMNS = "{table: {name: testname, pname: test_table, columns: [{ name: word1.word2, domain: domain1, nullable: false}, {name: word2.word3, domain: domain2, pkey: 2, nullable: false}, {name: word3.GGG, domain: domain1, pkey: 1, nullable: false}]}}"
  def setup
    @domains = Domains.new(
      YAML.load(
        "domains: {domain1: {type: number, size: 10, default: 200, nullable: false}, domain2: {type: varchar, size: 10}}"))
    @dict = Dictionary.new(
      YAML.load(
        "{dictionary: {word1: AAA, word2: BBB, word3: CCC}}"
      )
    )
  end

  def test_make_table
    table = Table.new(YAML.load(TABLE_SAMPLE), nil, @dict, @domains)
    assert_equal "AAABBB number(10) DEFAULT 200 NOT NULL", get_columns_def(table.columns)
    assert_equal "test_table", table.pname

    table = Table.new(YAML.load(TABLE_SAMPLE_NOPNAME), 'file_name.yaml', @dict, @domains)
    assert_equal "file_name", table.pname

  end

  def test_get_pkey
    table = Table.new(YAML.load(TABLE_SAMPLE_COLUMNS), nil, @dict, @domains)
    assert_equal "CCCGGG", table.pkey[0].pname
    assert_equal "BBBCCC", table.pkey[1].pname
  end

  def test_make_create_table
    table = Table.new(YAML.load(TABLE_SAMPLE), nil, @dict, @domains)
    assert_equal "drop table test_table;\ncreate table test_table (\n  AAABBB number(10) DEFAULT 200 NOT NULL\n\n);\n", get_ddl_table(table)

    table = Table.new(YAML.load(TABLE_SAMPLE_COLUMNS), nil, @dict, @domains)
    assert_equal "drop table test_table;\ncreate table test_table (\n  AAABBB number(10) DEFAULT 200 NOT NULL,\n  BBBCCC varchar(10)  NOT NULL,\n  CCCGGG number(10) DEFAULT 200 NOT NULL\n  , constraint PK_test_table primary key(CCCGGG, BBBCCC)\n);\n", get_ddl_table(table)
  end

  def test_make_table_comment
    table = Table.new(YAML.load(TABLE_SAMPLE_COLUMNS), nil, @dict, @domains)
    assert_equal "comment on table test_table is 'testname';\ncomment on column test_table.AAABBB is 'word1word2';\ncomment on column test_table.BBBCCC is 'word2word3';\ncomment on column test_table.CCCGGG is 'word3GGG';\n", get_ddl_table_comment(table)
  end
  
  def test_indexes
    index_test_table =
      "table: {name: test_index, pname: test_index, columns: [{pname: col1, type: number, nullable: true, index2: 2, index1: 1}, {pname: col2, type: number, index2: 1}, {pname: col3, type: number}]}"
    table = Table.new(YAML.load(index_test_table), nil, nil, nil)
    assert_equal "index1", table.index_keys[0]
    assert_equal "index2", table.index_keys[1]
    assert_equal "col1", table.index("index1").map{|e| e.pname}.join(",")
    assert_equal "col2,col1", table.index("index2").map{|e| e.pname}.join(",")
    assert_equal "create index I_test_index1 on test_index(col1);\ncreate index I_test_index2 on test_index(col2, col1);\n", get_index(table)

  end

  def test_table_checks
    table_with_check =  "{table: {name: testname, pname: test_table, columns: [{ name: word1.word2, domain: domain1}], checks: <word1.word2> > 0}}"
    table = Table.new(YAML.load(table_with_check), nil, @dict, @domains)
    assert_equal "AAABBB > 0", table.checks[0]

    table_with_check2 =  "{table: {name: testname, pname: test_table, columns: [{ name: word1.word2, domain: domain1, nullable: false}, {name: word2.word3, domain: domain2, pkey: 2, nullable: false}, {name: word3.GGG, domain: domain1, pkey: 1, nullable: false}], checks: [<word2.word3> = <word3.GGG>, <word3.GGG> < <word1.word2>]}}"
    table = Table.new(YAML.load(table_with_check2), nil, @dict, @domains)
    assert_equal "BBBCCC = CCCGGG", table.checks[0]
    assert_equal "CCCGGG < AAABBB", table.checks[1]
    assert_equal "drop table test_table;\ncreate table test_table (\n  AAABBB number(10) DEFAULT 200 NOT NULL,\n  BBBCCC varchar(10)  NOT NULL,\n  CCCGGG number(10) DEFAULT 200 NOT NULL\n  , constraint PK_test_table primary key(CCCGGG, BBBCCC)\n  , CHECK(BBBCCC = CCCGGG)\n  , CHECK(CCCGGG < AAABBB)\n);\n", get_ddl_table(table)

  end

  def test_refers

    refer_test_table = "{table: {name: test_refers, refers: [word1.word4, word2.word3], columns: [name: dummy]}}"
    table = Table.new(YAML.load(refer_test_table), nil, @dict, @domains)
    assert_equal "AAAword4", table.refers[0]
    assert_equal "BBBCCC", table.refers[1]

    refer_test_table = "{table: {columns: [name: dummy]}}"
    table = Table.new(YAML.load(refer_test_table), nil, @dict, @domains)
    assert_equal nil, table.refers

  end 

  def test_refer
    refer_test_table = "{table: {columns: [ {name: TEST1, refers: {table: 1, order: 999}}, {name: TEST2, refers: {table: 1}} ]}}"
    table = Table.new(YAML.load(refer_test_table), nil, @dict, @domains)
    assert_equal 999, table.refer(1)[1].order
    assert_equal "TEST1", table.refer(1)[1].column
    assert_equal "TEST2", table.refer(1)[0].column
  end

  def test_refer_sql
    refer_test_table = "{table: {pname: test_refers, refers: ptable1, columns: [ {name: TEST1, refers: {table: 0, order: 999}}, {name: TEST2, refers: {table: 0, column: TEST9}} ]}}"
    table = Table.new(YAML.load(refer_test_table), nil, @dict, @domains)

    assert_equal "alter table test_refers add foreign key(TEST2,TEST1) references ptable1(TEST9,TEST1)", get_ref_constraints(table)
  end


end
