# coding: utf-8

require 'yaml'
require 'optparse'

INDENT = " " * 2
CHECK_COLUMN_NAME = /<name>/ # 列内チェック制約の場合に列名に変換する値を示す
CHECK_REPLACE_COLUMN_NAME = /<([^\s<>]+)>/ # 列外チェック制約の場合に列名に変換する値を示す

module YAMLLoadable
  def extract_element(parsetree, element, raise_when_noexist = true)
    raise 'This root element is not a Hash Object.' if parsetree.instance_of?(Array)
    if (ret = parsetree[element])
      ret
    else
      raise "#{element} element doesn't exist." if raise_when_noexist
      nil
    end
  end

  def self.load_yaml(filename)
    (filename ? YAML.load_file(filename) : YAML.load(ARGF))
  end
end

class CheckConstraints
  class Check
    def initialize(counter, notation, column)
      @counter = counter
      @notation = notation
      @column = column
    end
  end
  def initialize(table)
    @table = table
    @checks = []
  end
  def add(notation, column = nil)
    @checks.push(CheckConstraints::Check.new(@checks.count, notation, column))
    @checks[-1]
  end
end

class LoadableElement
  include YAMLLoadable 

  def initialize(parsetree, filename = nil)
    @filename = filename
    @root = extract_element(parsetree, self.class.to_s.downcase)
  end

  # ファイルからロードする場合はこのクラスメソッドを使用する。
  def self.load(filename)
    new(YAMLLoadable::load_yaml(filename), filename)
  end

  def [](key)
    @root[key]
  end

  def dump(node = @root)
    YAML.dump(node)
  end

end

class Domains < LoadableElement
end

class Dictionary < LoadableElement
  def translate(string)
    # original_nameを.で分割し、dictで変換
    string.split('.').inject(""){|r, e| r + (@root[e] || e)}
  end
end

class Column
  attr_reader :name, :pname, :pkey, :type, :size, :default, :nullable, :comment, :indexes, :oname
  def initialize(parsetree, dict = nil, domains = nil)
    @root = parsetree
    @domain = @root["domain"]
    @oname = @root["name"] # Original name
    @name = @oname.delete('.') if @root["name"]
    merge_attribute(domains, @root["type"]) if domains
    @pname = @root["pname"] || make_pname(dict)
    @pkey = @root["pkey"]
    @type = @root["type"]
    @checks = @root["check"]
    @size = @root["size"]
    @default = @root["default"]
    @nullable = @root.has_key?("nullable") ? @root["nullable"] : true
    @comment = @root["comment"] || ""
    @indexes = @root.select{|key, val| key =~ /\Aindex\d+\Z/}
  end

  def checks
    return nil unless @checks
    @checks = [@checks] if !@checks.instance_of?(Array)
    return @checks.map do |e|
      if e.match(CHECK_COLUMN_NAME)
        e.gsub(CHECK_COLUMN_NAME, @pname)
      else
        @pname + ' ' + e
      end
    end
  end 

  class Reference
    attr_reader :table_index, :order, :column, :rcolumn
    def initialize(referhash, column_name)
      @column = column_name
      @table_index = referhash["table"]
      @order = referhash["order"] || 0
      @rcolumn = referhash["column"] || @column
    end
    def <=>(other)
      return self.order <=> other.order
    end
  end
    
  def refers
    refers = @root["refers"]
    return nil unless refers
    refers = [refers] unless refers.instance_of?(Array)
    refers.map{|e| Reference.new(e, @pname)}
  end

  private

  def make_pname(dict)
    return @oname unless dict
    if @oname
      dict.translate(@oname) 
    else
      @oname
    end
  end

  def merge_attribute(domains, type)
    # domain属性があれば、ドメインの定義を取得し、ドメインの属性で上書きする
    if @domain && (domain_elements = domains[@domain])
      @root = domain_elements.merge(@root)
    # type属性がない場合はname属性からドメイン定義を取得する。
    elsif !type && (domain_elements = domains[@name])
      @root = domain_elements.merge(@root)
    end
  end

end

class Table < LoadableElement
  attr_reader :columns

  def initialize(parsetree, filename, dict, domains)
    super(parsetree, filename)
    @columns = extract_element(@root, 'columns').map do |e|
      Column.new(e, dict, domains)
    end

    @checks = @root['checks']
    @dict = dict
  end

  def self.load(filename, dict, domains)
    new(YAMLLoadable::load_yaml(filename), filename, dict, domains)
  end

  def name
    @root["name"]
  end

  def pname
    @root["pname"] || File.basename(@filename, ".*")
  end

  def pkey
    @columns.select{|e| e.pkey}.sort{|x, y| x.pkey <=> y.pkey}
  end

  def index_keys
    keys = @columns.inject([]){|r, v| r + v.indexes.keys}.uniq.sort
  end

  def index(key)
    @columns.select{|e| e.indexes[key]}.sort{|x, y| x.indexes[key] <=> y.indexes[key]}
  end

  def refers
    refers = @root["refers"] 
    return nil unless refers
    refers = [refers] unless refers.instance_of?(Array)
    refers.map{|e| @dict.translate(e)}
  end

  def refer(table_index)
    columns = @columns.select{|e| e.refers}
    return nil unless columns
    columns.inject([]){|r, v| r + v.refers.select{|f| f.table_index == table_index}}.sort
  end

  def comment
    @root["comment"] || ""
  end

  def checks
    return nil unless @checks
    @checks = [@checks] if !@checks.instance_of?(Array)
    return @checks.map do |e|
      e.gsub(CHECK_REPLACE_COLUMN_NAME) do |column_name|
        checked_column = @columns.find(nil){|column| column.oname == $1}
        if checked_column
          checked_column.pname
        else
          ''
        end
      end
    end
  end

  
end

module Sql
  def get_ddl_table(table)

<<EOS
drop table #{table.pname};
create table #{table.pname} (
#{get_columns_def(table.columns, 1)}
#{get_tab_constraints(table, 1)}
);
EOS

  end
  def format_comment(comment)
    if comment == ""
      return ""
    else
      return "\t " + comment
    end
  end
  def get_ddl_table_comment(table, indent = 0)
    
    result = <<EOS
#{INDENT * indent}comment on table #{table.pname} is '#{table.name}#{format_comment(table.comment)}';
EOS
    table.columns.each do |e|
      result += <<EOS
#{INDENT * indent}comment on column #{table.pname}.#{e.pname} is '#{e.name}#{format_comment(e.comment)}';
EOS
    end
  result
  end

  def get_tab_constraints(table, indent = 0)
    ret_str = ""
    if table.pkey.size > 0
      ret_str = INDENT * indent + ", constraint PK_#{table.pname} primary key(#{table.pkey.map{|e| e.pname}.join(", ")})"
    end
    ret_str += get_check(table, "\n" + INDENT * indent + ", ")
  end

  def get_index(table)
    table.index_keys.inject("") do |r, e|
      r + "create index I_#{table.pname}#{e.match(/\d+$/)[0]} on #{table.pname}(#{table.index(e).map{|col| col.pname}.join(", ")});\n"
    end
  end

  def get_ref_constraints(table)
    refers = table.refers
    return nil unless refers
    sql = refers.map.with_index do |e, i|
      "alter table #{table.pname} add foreign key(#{table.refer(i).map(&:column).join(',')}) references #{e}(#{table.refer(i).map(&:rcolumn).join(',')})"
    end
    sql.join(";\n")
  end

  def get_check(checked_obj, sepstr = " ")
    return "" unless (checked_obj.checks)
    checked_obj.checks.inject(""){|r, i| r + sepstr + "CHECK(#{i})"}
  end

  def get_size(column)
    if (val = column.size)
      "(" + val.to_s + ")"
    else
        ""
    end
  end

  def get_default(column)
    if (val = column.default)
      "DEFAULT #{val}"
    else
       ""
    end
  end

  def get_null(column)
    (column.nullable ? "" : " NOT NULL") 
  end

  def get_columns_def(columns, indent = 0)
    (columns.map do |e|
        INDENT * indent + "#{e.pname} #{e.type}#{get_size(e)}"  +
        " #{get_default(e)}#{get_null(e)}#{get_check(e)}"
    end).join(",\n")
  end

end

if __FILE__ == $PROGRAM_NAME
  include Sql
  params = ARGV.getopts("", "dic:", "dom:", "tab:", "index")
  script_path = File.expand_path(File.dirname($0))

  tab = Table.load(
    params["tab"] || 'table.yaml', 
    Dictionary.load(params["dic"] || script_path + '/dict.yaml'),
    Domains.load(params["dom"] || script_path + '/domains.yaml')
  )
  if params["index"]
    puts Sql::get_index(tab).encode('cp932') if tab.index_keys.size > 0
  else
    puts Sql::get_ddl_table(tab).encode('cp932')
    puts Sql::get_ddl_table_comment(tab).encode('cp932')
  end

end
