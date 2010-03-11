require 'mongo'

module Sequel
  module Mongo
    class Database < Sequel::Database
      set_adapter_scheme :mongo
      
      def initialize(*args)
        super
        self.quote_identifiers = false
        self.identifier_input_method = nil
        self.identifier_output_method = nil
      end
      
      def connect(server)
        opts = server_opts(server)
        args = []
        if opts[:host]
          args << opts[:host]
          args << opts[:port] if opts[:port]
        end
        db = ::Mongo::Connection.new(*args).db(opts[:database])
        if opts[:username] && opts[:password]
          db.authenticate(opts[:username], opts[:password])
        end
        db
      end
      
      def dataset(opts = nil)
        Mongo::Dataset.new(self, opts)
      end
      
      def tables(opts={})
        synchronize(opts[:server]){|c| c.collection_names.map{|x| x.to_sym}}
      end

      private

      def disconnect_connection(c)
      end
    end
    
    class Dataset < Sequel::Dataset
      def count
        s = selector
        f = find_opts
        collection do |c|
          log("find(#{s.inspect}, #{f.inspect}).count")
          c.find(s, f).count
        end
      end
      
      def insert(h)
        h = mongo_db_hash(h)
        collection do |c|
          log("insert(#{h.inspect})")
          c.insert(h)
        end
      end
      
      def fetch_rows(sql)
        s = selector
        f = find_opts
        collection do |c|
          log("find(#{s.inspect}, #{f.inspect})")
          cols_set = !@columns
          c.find(s, f).each do |r|
            if cols_set
              @columns = sym_key_hash(r).keys
              cols_set = false
            end
            yield sym_key_hash(r)
          end
        end
      end
      
      def delete
        s = selector
        collection do |c|
          log("remove(#{s.inspect})")
          c.remove(s)
        end
      end
      
      UPDATE_OPTS = {:safe=>true}
      def update(values)
        s = selector
        h = {'$set'=>mongo_db_hash(values)}
        o = UPDATE_OPTS
        o = o.merge(:multi=>true) if s.empty?
        collection do |c|
          log("update(#{s.inspect}, #{h.inspect}, #{o.inspect})")
          v = c.update(s, h, o)
          UPDATE_OPTS[:safe] ? v.first.first["n"] : v
        end
      end
      
      [:join_table, :having, :group].each do |m|
        class_eval("def #{m}(*args); raise(Error, 'Not supported on MongoDB'); end", __FILE__, __LINE__)
      end
      
      # SQL fragment for specifying given CaseExpression.
      def case_expression_sql(ce)
        _case_expression_sql(ce.expression, ce.conditions, ce.default)
      end
      
      # SQL fragment for the SQL CAST expression.
      def cast_sql(expr, type)
        if type == String
          "(#{this_literal(expr)} + '')"
        elsif [Numeric, Integer, Fixnum, Bignum, Float].include?(type)
          "(#{this_literal(expr)} - 0)"
        else
          raise Error, "Only String, Numeric, Integer, Fixnum, Bignum, and Float are valid cast types on MongoDB"
        end
      end
      
      def _case_expression_sql(expression, conditions, default)
        cond, *rest = conditions
        c, r = cond
        n = rest.empty? ? this_literal(default) : _case_expression_sql(expression, rest, default)
        e = expression ? "(#{this_literal(expression)} == #{this_literal(c)})" : this_literal(c)
        "(#{e} ? #{this_literal(r)} : #{n})"
      end
      
      TWO_ARITY_OPS = SQL::ComplexExpression::INEQUALITY_OPERATORS + SQL::ComplexExpression::BITWISE_OPERATORS + [:"!=", :"=", :IS, :"IS NOT"]
      N_ARITY_OPS = [:AND, :OR, :'||'] + SQL::ComplexExpression::MATHEMATICAL_OPERATORS
      REGEXP_OPS = [:~, :'!~', :'~*', :'!~*']
      LIKE_OPS = [:LIKE, :ILIKE, :'NOT LIKE', :'NOT ILIKE']
      OP_MAP = {:"="=>'=='.freeze, :AND=>'&&'.freeze, :OR=>'||'.freeze, :'||'=>'+'.freeze, :IS=>'=='.freeze, :"IS NOT"=>'!='.freeze}
      def complex_expression_sql(op, args)
        case op
        when *N_ARITY_OPS
          "(#{args.collect{|a| this_literal(a)}.join(" #{OP_MAP.fetch(op, op)} ")})"
        when *TWO_ARITY_OPS
          "(#{this_literal(args.at(0))} #{OP_MAP.fetch(op, op)} #{this_literal(args.at(1))})"
        when *REGEXP_OPS
          "(#{'!' if op == :'!~' || op == :'!~*'}#{this_literal(args.at(0))}.match(/#{this_literal(args.at(1))[1...-1].gsub('/', '\\/')}/#{'i' if op == :'~*' || op == :'!~*'}))"
        when *LIKE_OPS
          "(#{'!' if op == :'NOT LIKE' || op == :'NOT ILIKE'}#{this_literal(args.at(0))}.match(/^#{Regexp.escape(this_literal(args.at(1))[1...-1]).gsub('%', '.*').gsub('_', '.')}$/#{'i' if op == :'ILIKE' || op == :'NOT ILIKE'}))"
        when :NOOP
          this_literal(args.at(0))
        when :NOT
          "!#{this_literal(args.at(0))}"
        when :'B~'
          "~#{this_literal(args.at(0))}"
        when :IN, :"NOT IN"
          "(#{this_literal(args.at(1))}.indexOf(#{this_literal(args.at(0))}) #{op == :IN ? '!=' : '=='} -1)"
        else
          super
        end
      end
      
      private
      
      def collection
        db.synchronize(opts[:server]) do |c|
          begin
            yield c[literal(first_source_alias)]
          rescue ::Mongo::MongoDBError, ::Mongo::MongoRubyError, ::Mongo::InvalidName, ArgumentError, RuntimeError => e
            db.send(:raise_error, e)
          end
        end
      end
      
      def find_opts
        o = @opts
        h = {}
        h[:limit] = o[:limit] if o[:limit]
        h[:skip] = o[:offset] if o[:offset]
        h[:fields] = o[:select].map{|c| literal(c)} if o[:select] && !o[:select].empty?
        h[:sort] = o[:order].map{|c| [(c.is_a?(SQL::OrderedExpression) ? c.expression : c), (c.is_a?(SQL::OrderedExpression) && c.descending) ? :desc : :asc]} if o[:order]
        h
      end
      
      def log(message)
        db.log_info("DB[#{first_source_alias.inspect}].#{message}")
      end
      
      def this_literal(s)
        case s
        when Symbol, SQL::Identifier
          "this.#{literal(s)}"
        when SQL::QualifiedIdentifier
          s.table == first_source_alias ? this_literal(s.column) : literal(s)
        else
          literal(s)
        end
      end
      
      def literal_array(a)
        "[#{a.map{|x| this_literal(x)}.join(', ')}]"
      end
      
      def literal_false
        'false'
      end
      
      def literal_nil
        'undefined'
      end
      
      def literal_true
        'true'
      end
      
      def select_sql
        nil
      end
      
      def selector
        (w = @opts[:where]) ? {'$where'=>this_literal(w)} : {}
      end
      
      def mongo_db_hash(values)
        h = {}
        values.each{|k,v| h[literal(k)] = v}
        h
      end
      
      def sym_key_hash(values)
        h = {}
        values.each{|k,v| h[k.to_sym] = v}
        h
      end
    end
  end
end
