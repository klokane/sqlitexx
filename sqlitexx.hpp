#ifndef SQLITEXX_H_7RBKBKCH

#define SQLITEXX_H_7RBKBKCH

#include <sqlite3.h>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>


//  Copyright Jiri Kratochvil (aka KLoK) 2010
//  Distributed under the Boost Software License, Version 1.0.
//  (see at http://www.boost.org/LICENSE_1_0.txt)

namespace sqlitexx {

namespace error {
class SQLiteError : public std::runtime_error {
public:
  explicit SQLiteError(int code, const std::string& msg = "") 
    : std::runtime_error(format(code,msg)) {
  }

  virtual ~SQLiteError() throw() {}

protected:
  std::string format(int code, const std::string& msg) const {
    using boost::format;
    return str(format("SQLITE[%d]: %s (%s)")
        % code
        % toString(code)
        % msg
        );
  }

  const char* toString(int code) const {
    switch(code) {
      case SQLITE_ERROR:      return "SQL error or missing database";
      case SQLITE_INTERNAL:   return "Internal logic error in SQLite"; 
      case SQLITE_PERM:       return "Access permission denied";
      case SQLITE_ABORT:      return "Callback routine requested an abort";
      case SQLITE_BUSY:       return "The database file is locked";
      case SQLITE_LOCKED:     return "A table in the database is locked";
      case SQLITE_NOMEM:      return "A malloc() failed";
      case SQLITE_READONLY:   return "Attempt to write a readonly database";
      case SQLITE_INTERRUPT:  return "Operation terminated by sqlite3_interrup";
      case SQLITE_IOERR:      return "Some kind of disk I/O error occurred";
      case SQLITE_CORRUPT:    return "The database disk image is malformed";
      case SQLITE_NOTFOUND:   return "Unknown opcode in sqlite3_file_control()";
      case SQLITE_FULL:       return "Insertion failed because database is full";
      case SQLITE_CANTOPEN:   return "Unable to open the database file";
      case SQLITE_PROTOCOL:   return "Database lock protocol error";
      case SQLITE_EMPTY:      return "Database is empty";
      case SQLITE_SCHEMA:     return "The database schema changed";
      case SQLITE_TOOBIG:     return "String or BLOB exceeds size limit";
      case SQLITE_CONSTRAINT: return "Abort due to constraint violation";
      case SQLITE_MISMATCH:   return "Data type mismatch";
      case SQLITE_MISUSE:     return "Library used incorrectly";
      case SQLITE_NOLFS:      return "Uses OS features not supported on host";
      case SQLITE_AUTH:       return "Authorization denied";
      case SQLITE_FORMAT:     return "Auxiliary database format error";
      case SQLITE_RANGE:      return "2nd parameter to sqlite3_bind out of range";
      case SQLITE_NOTADB:     return "File opened that is not a database file";
    }
  return "Unknown code";
  }
};
}; /* namespace error */

template <typename T>
class holder;

class value {
public:
  value(){}
  virtual ~value(){}

  template<typename T> 
  T& get() {
    holder<T>*p = dynamic_cast<holder<T>*>(this);
    if(!p) throw std::bad_cast();
    return p->val<T>();
  }
private:
  template<typename T> T val();
};

// FIXME: if not polymorfic just copy valua - do not allocate on heap
template <typename T>
class holder : public value {
public:
  holder(T* t) : val_(t) {}
  ~holder() { delete val_; }

  template<typename U> U& val() const { return *val_; }
private:
  T* val_;
};


class row {
public:
  virtual ~row() {
    for ( columns_t::iterator i = columns_.begin() ; i != columns_.end()  ; ++i ) {
      delete i->second;
    }
    columns_.clear();
  }

  template<class T>
  const T& get(const std::string& key) const {
    columns_t::const_iterator i = columns_.find(key);
    if(i == columns_.end()) throw std::range_error("nonexistent index requested");
    return i->second->get<T>();
  }

  int size() const {
    return columns_.size();
  }

  template<class T>
  void add(const std::string& key, T* val) {
    columns_t::const_iterator i = columns_.find(key);
    if(i != columns_.end()) delete i->second;
    columns_[key] = new holder<T>(val);
  }

private:
  typedef std::map<std::string,value*> columns_t;
  columns_t columns_;
};

namespace binding {

  class binder {
  public:
    binder(sqlite3_stmt* const& s) : statement_(s) {}
    template<class T>
    void operator()(const T& val) ;
    void reset() { bound_.clear(); }

  private:
    void check(int rc) {
      if(rc) throw error::SQLiteError(rc);
      bound_.push_back(true);
    }
    std::vector<bool> bound_;
    sqlite3_stmt* const& statement_;
  };

  // TODO: allow binding params througth name

  template<>
  void binder::operator()<const char*>(const char* const& val) {
    check(sqlite3_bind_text(statement_,bound_.size()+1,val,strlen(val)+1,SQLITE_TRANSIENT));
  }

  template<>
  void binder::operator()<std::string>(const std::string& val) {
    check(sqlite3_bind_text(statement_,bound_.size()+1,val.c_str(),val.size()+1,SQLITE_TRANSIENT));
  }

  template<>
  void binder::operator()<int>(const int& val) {
    check(sqlite3_bind_int(statement_,bound_.size()+1,val));
  }

  template<>
  void binder::operator()<long>(const long& val) {
    check(sqlite3_bind_int64(statement_,bound_.size()+1,val));
  }

  template<>
  void binder::operator()<long long>(const long long& val) {
    check(sqlite3_bind_int64(statement_,bound_.size()+1,val));
  }

  template<>
  void binder::operator()<double>(const double& val) {
    check(sqlite3_bind_double(statement_,bound_.size()+1,val));
  }

  template<>
  void binder::operator()<boost::uuids::uuid>(const boost::uuids::uuid& val) {
    std::stringstream s; s << val;
    (*this)(s.str());
  }

  class extractor {
  public:
    extractor(sqlite3_stmt* const& s) : statement_(s) {}
    template<class T>
    void operator()(T& val);
    void reset() { extracted_.clear(); }

  private:
    sqlite3_stmt* const& statement_;
    std::vector<bool> extracted_;
  };

  template<>
  void extractor::operator()<std::string>(std::string& into) {
    const char* s = (const char*)sqlite3_column_text(statement_,extracted_.size());
    if(s) into.assign(s);
    extracted_.push_back(true);
  }

  template<>
  void extractor::operator()<int>(int& into) {
    into = sqlite3_column_int(statement_,extracted_.size());
    extracted_.push_back(true);
  }

  template<>
  void extractor::operator()<long>(long& into) {
    into = sqlite3_column_int(statement_,extracted_.size());
    extracted_.push_back(true);
  }

  template<>
  void extractor::operator()<double>(double& into) {
    into = sqlite3_column_double(statement_,extracted_.size());
    extracted_.push_back(true);
  }

  template<>
  void extractor::operator()<long long>(long long& into) {
    into = sqlite3_column_int64(statement_,extracted_.size());
    extracted_.push_back(true);
  }

  template<>
  void extractor::operator()<row>(row& into) {
    int cnt = sqlite3_column_count(statement_);
    while (cnt > extracted_.size()) {
      std::size_t index = extracted_.size();
      const char* ctype = sqlite3_column_decltype(statement_,extracted_.size());
      const char* cname = sqlite3_column_name(statement_,index);

      if(!strcmp(ctype,"TEXT")) {
        std::string* v = new std::string; 
        (*this)(*v);
        into.add(cname,v);
      } else if(!strcmp(ctype,"FLOAT")) {
        // TODO: handle non-polymorfic types just only by copy value instead of dynamic alloc
        double* v = new double;
        (*this)(*v);
        into.add(cname,v);
      } else if(!strcmp(ctype,"INTEGER")) {
        // TODO: handle non-polymorfic types just only by copy value instead of dynamic alloc
        int* v = new int;
        (*this)(*v);
        into.add(cname,v);
      } else {
        throw error::SQLiteError(SQLITE_ERROR,"Unsuported conversion");
      }
    }
  }

} /* namespace binding */


class db;
class statement {
  sqlite3_stmt* statement_;
  const db& db_;
public:
  statement(const db& database ,const std::string& sql);
  virtual ~statement(){ finalize(); }

  void exec();

  template<typename T>
  bool fetch(T& into);

  void reset() {
    if(statement_) {
      int rc = sqlite3_reset(statement_);
      if(rc) throw error::SQLiteError(rc,"finalize");
      binder_.reset();
    }
  }

  void finalize() {
    if(statement_) {
      int rc = sqlite3_finalize(statement_);
      statement_ = NULL;
      binder_.reset();
    }
  }

  template <class T>
  statement& operator%(const T& t) {
    binder_(t);
    return *this;
  }

private:
  std::string sql_;
  binding::binder binder_;
  binding::extractor extractor_;
};

class db {
  sqlite3* handler_;
  std::string dbpath_;
public:
  db(const std::string& path) : dbpath_(path) {
    int rc = sqlite3_open(dbpath_.c_str(),&handler_);
    if(rc) throw error::SQLiteError(rc,sqlite3_errmsg(handler_));
  }
  ~db() {
    sqlite3_close(handler_);
  }

  statement query(const std::string& sql) {
    return statement(*this,sql);
  }

  friend class statement;
};


statement::statement(const db& database ,const std::string& sql) 
  :sql_(sql) 
  ,db_(database)
  ,statement_(NULL)
  ,binder_(statement_)
  ,extractor_(statement_)
{
  const char* tail = NULL;
  int rc = sqlite3_prepare(db_.handler_
      ,sql_.c_str()
      ,-1
      ,&statement_
      ,&tail);
  if(rc) throw error::SQLiteError(rc,sqlite3_errmsg(db_.handler_));
}

void statement::exec() {
  int rc = sqlite3_step(statement_);
  if(rc == SQLITE_ROW) throw error::SQLiteError(rc,"use fetch() instead of exec()");
  if(rc != SQLITE_DONE) throw error::SQLiteError(rc,sql_);
}

template<typename T>
bool statement::fetch(T& into) {
  int rc = sqlite3_step(statement_);
  if((rc != SQLITE_ROW) && (rc != SQLITE_DONE)) throw error::SQLiteError(rc);
  extractor_.reset();
  extractor_(into);
  return rc == SQLITE_ROW;
}

template <typename T>
class rowset_iterator {
public:
  typedef std::input_iterator_tag iterator_category;
  typedef T   value_type;
  typedef T* pointer;
  typedef T& reference;
  typedef ptrdiff_t difference_type;

  rowset_iterator() : statement_(0), value_(0) {}
  rowset_iterator(statement& st, reference val) 
    : statement_(&st), value_(&val) {
    ++(*this);
  }

  rowset_iterator & operator++()
  {
      if (statement_->fetch(*value_) == false) {
          statement_ = 0;
          value_ = 0;
      }

      return (*this);
  }

  reference operator*() const {
      return (*value_);
  }

  pointer operator->() const {
      return &(operator*());
  }

  bool operator==(rowset_iterator const & other) const {
      return (statement_ == other.statement_)
        && (value_ == other.value_);
  }

  bool operator!=(rowset_iterator const & other) const {
      return !(*this == other);
  }

private:
  statement* statement_;
  pointer value_;

};

template<class T = row>
class rowset {
public:
  typedef T value_type;
  typedef rowset_iterator<T> iterator;
  //typedef rowset_iterator<T> const_iterator;

  rowset(statement& stmt) : statement_(stmt) {
  }

  iterator begin() {
    return iterator(statement_,current_);
  }

  iterator end() {
    return iterator();
  }

private:
  statement& statement_;
  value_type current_;
};
  
} /* litesqlxx */


#endif /* end of include guard: SQLITEXX_H_7RBKBKCH */
