#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

// This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.

// This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

// You should have received a copy of the GNU General Public License along with this program. If not, see <https://www.gnu.org/licenses/>. 

extern "C" {
#include <assert.h>

#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stddef.h>
#include <fcntl.h>
#include <stdarg.h>
#include <dirent.h>
#include <stdint.h>
#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <unistd.h>
#include <libgen.h>

#include <zlib.h>
}

#include <algorithm>
#include <array>
#include <exception>
#include <sstream>
#include <utility>
#include <unordered_set>
#include <unordered_map>
#include <string>
#include <vector>
#include <fstream>
#include <memory>
#include <algorithm>

using namespace std;

////////////////////////////////////////////////////////////////////////

class GzFileHandler;
class DirResult;
class TopResult;
class DirWalkState;
class DiskUsage;
class FileError;
class ZlibFailed;
class BadFile;

template<class T> T above_int(T t,int i) {
  if(t<i)
    return T(i);
  else
    return t;
}

void ezwrite(gzFile out,const char *data,int64_t len);
void ezread(gzFile in,char *data,int64_t len);
void write_size_string(gzFile o,const string &s);
void read_size_string(gzFile in,string &s);
void be_verbose();
void be_quiet();
void set_restart_interval(int64_t seconds);
int debug(const char *format,...);
int info(const char *format,...);
int warning(const char *format,...);
int error(const char *format,...);
string random_base36_string();
string strdirname(char *path);
string strbasename(char *path);
string strrealpath(const string &s);
string strstrftime(const char *format,const struct tm *tm=NULL);
string strhostname();
bool write_xml_report_entry(FILE *tgt,
                            const char *tag,
                            const char *indent,
                            const char *name,
                            const char *valid_time,
                            shared_ptr<const TopResult> sub);
bool write_xml_report(const string &where,const DiskUsage &du);

////////////////////////////////////////////////////////////////////////

class GzFileHandler {
public:
  GzFileHandler(const string &path,const string &tempfile,const string &mode);
  virtual ~GzFileHandler();
  void close();
  gzFile get() { return file; }
private:
  gzFile file;
  string path,mode,tempfile;
  bool have_tempfile;
};

struct FileInfo {
  int64_t bytes, blocks, files;
  FileInfo(): bytes(0), blocks(0), files(0) {}
  FileInfo(const FileInfo &o): bytes(o.bytes), blocks(o.blocks), files(o.files) {}
  FileInfo(std::initializer_list<int64_t> list) {
    auto it = list.begin();
    bytes = *it++;
    blocks = *it++;
    files = *it++;
  }
  FileInfo &operator += (const FileInfo &o) {
    bytes+=o.bytes;
    blocks+=o.blocks;
    files+=o.files;
    return *this;
  }
};

class DirResult {
  // A simple container that tracks the size of a directory
public:
  static const int age_count = 3;
  static const time_t age_seconds[age_count];

  DirResult() {};
  //  explicit DirResult(FileInfo f);
  explicit DirResult(gzFile in);
  DirResult(const DirResult &o): result(o.result) {}
  void write_restart(gzFile out) const;
  virtual ~DirResult() {}
  DirResult &add(int age,const FileInfo &fi);
  FileInfo &at_age(int age) { return result[age]; }
  const FileInfo &at_age(int age) const { return result[age]; }
  const array<FileInfo,age_count> &get_result() const { return result; }
  DirResult &operator += (const DirResult &d);
private:
  array<FileInfo,age_count> result;
};

class TopResult: public DirResult {
  // Tracks the size of a directory AND keeps the du output in an
  // ostringstream.
public:
  TopResult(const string &name,const string &out);
  explicit TopResult(gzFile in);
  void write_restart(gzFile out) const;
  void write_content(const string &filename);

  virtual ~TopResult() {};
  string get_content() const { return content.str(); }
  ostringstream &add_content() { return content; }
  const string &get_name() const { return name; }
  time_t get_finish_time() const { return finish_time; }
  void set_finish_time() { finish_time=time(NULL); }
private:
  ostringstream content;
  string name;
  int64_t finish_time;
};

class DirWalkState: public DirResult {
  // Tracks the size of a directory, its name, and the list of
  // subdirectories already processed.
public:
  DirWalkState(const string &reldir,const string &path);
  DirWalkState(gzFile in);
  void write_restart(gzFile out) const;

  virtual ~DirWalkState() {}
  bool is_done(const string &d) const { return done.find(d)!=done.end(); }
  void mark_done(const string &d) { done.insert(d); }
  void clear_done() { done.clear(); }
  const string &get_reldir() const { return reldir; }
  const string &get_path() const { return path; }
private:

  string reldir, path;
  unordered_set<string> done;
};

class DiskUsage {
public:
  DiskUsage(const string &path,const string &restart_file,const string &output_base,bool restart=false);
  DirResult tree_walk();
  int64_t time_elapsed() const;
  void sort_results_by_size();
  typedef vector< shared_ptr<TopResult> >::iterator iterator;
  typedef vector< shared_ptr<TopResult> >::const_iterator const_iterator;
  iterator results_begin() { return ordered_results.begin(); }
  const_iterator results_begin() const { return ordered_results.begin(); }
  iterator results_end() { return ordered_results.end(); }
  const_iterator results_end() const { return ordered_results.end(); }
  const string &top_dir() const { return topdir; }
  shared_ptr<const TopResult> result_for(const string &name) const;
private:
  float scan_files_and_list_subdirs(
      const string &reldir,const string &path,
      int walk_index,shared_ptr<TopResult> tr,bool restart,
      vector<string> &subdirs);
  DirResult tree_walk(const string &reldir,const string &path,int walk_index,shared_ptr<TopResult>,bool restart);
  shared_ptr<TopResult> result_for(const string &name);
  void clear();
  bool read_restart();
  void read_restart_impl(gzFile file);
  bool write_restart() const;
  void write_restart_impl(gzFile file) const;
  bool should_write_restart() const;
private:
  mutable time_t last_restart_write=-1;
  int64_t start_time,end_time;
  vector<DirWalkState> state;
  unordered_set<ino_t> ino_seen;
  string topdir, restart_path, output_base;
  vector < shared_ptr<TopResult> > ordered_results;
  unordered_map < string,shared_ptr<TopResult> > hashed_results;
  bool have_read_restart;

  static const int64_t magic_number=0x3031130330311303ll;
};

class FileError: public exception {
public:
  FileError() {}
  virtual ~FileError() {}
};

class ZlibFailed: public FileError {
public:
  ZlibFailed(const string &why): why(why) {}
  ZlibFailed(gzFile file) {
    int errnum=Z_ERRNO;
    const char *from_gzerror=NULL;

    if(file)
      from_gzerror=gzerror(file,&errnum);
    if(errnum==Z_ERRNO)
      why=strerror(errno);
    else if(from_gzerror && from_gzerror[0])
      why=from_gzerror;
    else if(from_gzerror && !from_gzerror[0] && errno)
      why=strerror(errno);
    else
      why="zlib call failed for no known reason (gzerror returned an empty string, and errno is zero)";
  }
  virtual ~ZlibFailed() {}
  virtual const char *what() const throw() {
    return why.c_str();
  }
private:
  string why;
};

class BadFile: public FileError {
public:
  BadFile(const string &s): why(s) {}
  virtual ~BadFile() {};
  virtual const char *what() const throw() { return why.c_str(); };
private:
  string why;
};

////////////////////////////////////////////////////////////////////////

// Globals

static int g_verbose=0;
static int64_t g_restart_interval=300;
static double g_min_files_per_second=15;

static string g_report_file = "";

static int64_t g_slow_io_check_interval = 30;

////////////////////////////////////////////////////////////////////////

// Functions

string append_path(const string &parent, const string &child) {
  string result = parent;
  if(parent.back()!='/') 
    result+='/';
  result+=child;
  return result;
}

string strdirname(const char *path) {
  size_t len=strlen(path)+1;
  unique_ptr<char[]> buf(new char[len+1>PATH_MAX+1 ? len+1 : PATH_MAX+1]);
  memcpy(buf.get(),path,len);
  char *result=dirname(buf.get());
  if(!result)
    return string(".");
  return string(result);
}

string strbasename(const char *path) {
  size_t len=strlen(path)+1;
  unique_ptr<char[]> buf(new char[len+1>PATH_MAX+1 ? len+1 : PATH_MAX+1]);
  memcpy(buf.get(),path,len);
  char *result=basename(buf.get());
  if(!result)
    return string(".");
  return string(result);
}

string strrealpath(const string &s) {
  unique_ptr<char[]> buf(new char[PATH_MAX+1]);
  memset(buf.get(),0,PATH_MAX+1);
  if(realpath(s.c_str(),buf.get()))
    return buf.get();
  string base=strbasename(s.c_str()),dir=strdirname(s.c_str());
  return strrealpath(dir)+'/'+base;
}

string strhostname() {
  unique_ptr<char[]> buf(new char[HOST_NAME_MAX+1]);
  memset(buf.get(),0,HOST_NAME_MAX+1);
  int result=gethostname(buf.get(),HOST_NAME_MAX);
  if(result && result!=ENAMETOOLONG && result!=EINVAL) {
    warning("gethostname failed: %s\n",strerror(errno));
    return "???";
  }
  buf.get()[HOST_NAME_MAX]='\0';
  return string(buf.get());
}

void ezwrite(gzFile out,const char *data,int64_t len) {
  const int64_t block_size=131072;
  while(len>0) {
    unsigned write=block_size;
    if(len<block_size)
      write=len;
    assert(write>0);
    int result=gzwrite(out,data,write);
    if(write<=0)
      throw ZlibFailed(out);
    else if((int64_t)result<(int64_t)write)
      throw ZlibFailed("short write (not enough disk space?)");
    data+=write;
    len-=write;
  }
}

void ezread(gzFile in,char *data,int64_t len) {
  const int64_t block_size=131072;
  while(len>0) {
    unsigned read=block_size;
    if(len<block_size)
      read=len;
    assert(read>0);
    int result=gzread(in,data,(int)read);
    if(result<0)
      throw ZlibFailed(in);
    else if((int64_t)result<(int64_t)read)
      throw ZlibFailed("unexpected end of file");
    data+=read;
    len-=read;
  }
}

void write_size_string(gzFile out,const string &s) {
  int64_t size=s.size()+1;
  ezwrite(out,(const char *)&size,8);
  ezwrite(out,s.c_str(),size);
}

void read_size_string(gzFile in,string &s) {
  int64_t size;
  ezread(in,(char *)&size,8);
  std::unique_ptr<char[]> buf(new char[size+1]);
  if(!buf)
    throw ZlibFailed("cannot allocate buffer for read");
  ezread(in,buf.get(),size);
  s=buf.get();
}

void be_verbose() {
  g_verbose=1;
}

void be_quiet() {
  g_verbose=-1;
}

void set_restart_interval(int64_t interval) {
  if(interval<10)
    warning("warning: minimum restart interval is 10 seconds; setting to 10\n");
  g_restart_interval=above_int(interval,10);
}

void set_slow_io_check_interval(int64_t interval) {
  if(interval<10)
    warning("warning: minimum slow check interval is 10 seconds; setting to 10\n");
  g_slow_io_check_interval = above_int(interval,10);
}

int debug(const char *format,...) {
  va_list ap;
  if(g_verbose<1)
    return 0;
  va_start(ap,format);
  int ret=vfprintf(stderr,format,ap);
  va_end(ap);
  return ret;
}

int info(const char *format,...) {
  va_list ap;
  if(g_verbose<0)
    return 0;
  va_start(ap,format);
  int ret=vfprintf(stderr,format,ap);
  va_end(ap);
  return ret;
}

int warning(const char *format,...) {
  va_list ap;
  if(g_verbose<-1)
    return 0;
  va_start(ap,format);
  int ret=vfprintf(stderr,format,ap);
  va_end(ap);
  return ret;
}

int error(const char *format,...) {
  va_list ap;
  if(g_verbose<-2)
    return 0;
  va_start(ap,format);
  int ret=vfprintf(stderr,format,ap);
  va_end(ap);
  return ret;
}

string random_base36_string() {
  const char *chars="0123456789abcdefghijklmnopqrstuvwxyz";
  char junk[13]={0};
  for(int i=0;i<12;i++)
    junk[i]=chars[(rand()/1000)%36];
  return junk;
}

string strstrftime(const char *format,const struct tm *tm) {
  struct tm my_tm;
  if(!tm) {
    time_t now=time(NULL);
    if(!gmtime_r(&now,&my_tm))
      memset(&my_tm,0,sizeof(tm));
    tm=&my_tm;
  }
  char buf[100]={0};
  if(0>=strftime(buf,100,format,tm))
    return string("???");
  return string(buf);
}

string xml_cleaned(const string &dirty) {
  static const string keys = "&<";
  size_t there = dirty.find_first_of(keys);

  // Optimization for most common case: no bad characters.
  if(there == string::npos)
    return dirty;

  ostringstream clean;
  const size_t len = dirty.size();

  for(size_t pos = 0; pos < len ; there = dirty.find_first_of(keys, pos)) {
    if(there == string::npos) {
      // No more bad characters
      clean << dirty.substr(pos);
      break;
    } else {
      // Bad character found.

      // Send all text before the bad character.
      if(there > pos)
        clean << dirty.substr(pos, there - pos);

      // Use an entity for the bad character.
      if(dirty[there] == '<')
        clean << "&lt;";
      else if(dirty[there] == '&')
        clean << "&amp;";

      // Move pointer to the next character to process.
      pos = there + 1;
    }
  }

  return clean.str();
}

bool write_xml_report_entry(FILE *tgt,
                            const char *tag,
                            const char *indent,
                            const char *name,
                            const char *valid_time,
                            shared_ptr<const TopResult> sub) {
  int err;
  using llu_type = long long unsigned int;
  err = fprintf(tgt,
                "%s<%s>\n"
                "%s  <path>%s</path>\n"
                "%s  <valid>%s</valid>\n"
                "%s  <total>\n"
                "%s    <bytes>%f</bytes>\n"
                "%s    <blocks>%llu</blocks>\n"
                "%s    <files>%f</files>\n"
                "%s  </total>\n"
                "%s  <files_older_than age_in_seconds=\"%f\">\n"
                "%s    <bytes>%f</bytes>\n"
                "%s    <blocks>%llu</blocks>\n"
                "%s    <files>%f</files>\n"
                "%s  </files_older_than>\n"
                "%s  <files_older_than age_in_seconds=\"%f\">\n"
                "%s    <bytes>%f</bytes>\n"
                "%s    <blocks>%llu</blocks>\n"
                "%s    <files>%f</files>\n"
                "%s  </files_older_than>\n",
                indent, tag,
                indent, name,
                indent, valid_time,
                indent,
                indent, double(sub->at_age(0).bytes),
                indent, llu_type(sub->at_age(0).blocks),
                indent, double(sub->at_age(0).files),
                indent,
                indent, double(sub->age_seconds[1]),
                indent, double(sub->at_age(1).bytes),
                indent, llu_type(sub->at_age(1).blocks),
                indent, double(sub->at_age(1).files),
                indent,
                indent, double(sub->age_seconds[2]),
                indent, double(sub->at_age(2).bytes),
                indent, llu_type(sub->at_age(2).blocks),
                indent, double(sub->at_age(2).files),
                indent);
  return err > 0;
}

bool write_xml_report(const string &where,const DiskUsage &du) {
  FILE *tgt=stdout;
  string tempfile("(*stdout*)");
  bool have_tempfile=false;

  auto error_cleanup = [&]() {
    error("%s: cannot write data: %s",tempfile.c_str(),strerror(errno));
    if(have_tempfile) unlink(tempfile.c_str());
    if(tgt) fclose(tgt);
  };

  if(where.size()) {
    have_tempfile=true;
    tempfile=where+"-"+random_base36_string()+".tmp";
    tgt=fopen(tempfile.c_str(),"wt");
    if(!tgt) {
      error("%s: cannot open for text writing: %s\n",tempfile.c_str(),strerror(errno));
      return false;
    }
  }

  fprintf(tgt, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\n");

  fprintf(tgt, "<usage>\n");

  {
    // Write the result for the top level (".")
    shared_ptr<const TopResult> dot = du.result_for(".");
    string top_dir = xml_cleaned(strrealpath(du.top_dir()));
    const char *indent = "  ";
    string now = strstrftime("%a %d %b %Y %T %Z");

    if(!write_xml_report_entry(tgt, "dir", indent, top_dir.c_str(), now.c_str(), dot)) {
      error_cleanup();
      return false;
    }
  }
  
  for(auto it=du.results_begin();it!=du.results_end();it++) {
    const char *indent = "    ";

    string name = xml_cleaned((*it)->get_name());
    if(name==".")
      continue;

    time_t at_t=(*it)->get_finish_time();
    struct tm at_tm;
    if(!gmtime_r(&at_t,&at_tm))
      memset(&at_tm,0,sizeof(tm));
    string at=strstrftime("%a %d %b %Y %T %Z",&at_tm);

    if(!write_xml_report_entry(tgt, "subdir", indent, name.c_str(), at.c_str(), *it)) {
      error_cleanup();
      return false;
    }

    if(0 >= fprintf(tgt, "    </subdir>\n")) {
      error_cleanup();
      return false;
    }
  }

  if(0 >= fprintf(tgt, "  </dir>\n</usage>\n")) {
    error_cleanup();
    return false;
  }

  if(have_tempfile && rename(tempfile.c_str(),where.c_str())) {
    error_cleanup();
    return false;
  }

  if(tgt) fclose(tgt);
  return true;
}


////////////////////////////////////////////////////////////////////////

// Class GzFileHandler

GzFileHandler::GzFileHandler(const string &path,const string &tempfile,const string &mode):
  file(NULL),path(path),mode(mode),tempfile(tempfile),have_tempfile(tempfile.length()>0)
{
  const char *filename = have_tempfile ? tempfile.c_str() : path.c_str();
  file=gzopen(filename,mode.c_str());
  if(!file)
    throw ZlibFailed(file);
  if(gzbuffer(file,131072))
    throw ZlibFailed(file);
}

GzFileHandler::~GzFileHandler() {
  try {
    close();
  } catch(const ZlibFailed &z) {
    error("ERROR: failed to close a gzlib file: %s\n",z.what());
  } catch(const bad_alloc &b) {
    error("ERROR: bad_alloc; ran out of memory\n");
  } catch(...) {
    error("Uncaught C++ exception.\n");
  }
}

void GzFileHandler::close() {
  try {
    if(file) {
      int result=gzclose(file);
      file=NULL;
      switch(result) {
      case Z_STREAM_ERROR: throw ZlibFailed("Error on close: file is not valid");
      case Z_MEM_ERROR: throw ZlibFailed("Error on close: out of memory");
      case Z_BUF_ERROR: throw ZlibFailed("Error on close: last read ended in the middle of a gzip stream");
      case Z_OK: break;
      default: throw ZlibFailed("Unknown error on close.");
      };
    }
    if(have_tempfile) {
      if(rename(tempfile.c_str(),path.c_str()))
        throw ZlibFailed("Unable to rename gzipped file \""+tempfile+"\" to \""+path+"\": "+strerror(errno));
      have_tempfile=false;
    }
  } catch(const FileError &fe) {
    if(have_tempfile)
      unlink(tempfile.c_str());
    throw;
  }
}

////////////////////////////////////////////////////////////////////////

// Class DirResult

// File ages to track in seconds. Negative numbers means "all ages." Must be monotonically increasing.
const time_t DirResult::age_seconds[DirResult::age_count] = { -1, 7776000, 15552000 };

DirResult::DirResult(gzFile in)
{
  for(auto &bbf : result) {
    ezread(in,(char *)&bbf.bytes,8);
    ezread(in,(char *)&bbf.blocks,8);
    ezread(in,(char *)&bbf.files,8);
  }
}

void DirResult::write_restart(gzFile out) const {
  for(auto &bbf : result) {
    ezwrite(out,(const char *)&bbf.bytes,8);
    ezwrite(out,(const char *)&bbf.blocks,8);
    ezwrite(out,(const char *)&bbf.files,8);
  }
}

DirResult &DirResult::add(int age, const FileInfo &toAdd) {
  result[age] += toAdd;
  return *this;
}

DirResult &DirResult::operator += (const DirResult &d) {
  for(int i=0; i<age_count; ++i)
    result[i] += d.result[i];
  return *this;
}

////////////////////////////////////////////////////////////////////////

// Class TopResult

TopResult::TopResult(const string &name,const string &out):
  DirResult(),name(name),finish_time(-1)
{
  content<<out;
}
TopResult::TopResult(gzFile in):
  DirResult(in),content()
{
  ezread(in,(char*)&finish_time,8);
  read_size_string(in,name);
  string from;
  read_size_string(in,from);
  content<<from;
}
void TopResult::write_restart(gzFile out) const {
  this->DirResult::write_restart(out);
  ezwrite(out,(const char*)&finish_time,8);
  write_size_string(out,name);
  string c=get_content();
  write_size_string(out,c);
}
void TopResult::write_content(const string &filename) {
  debug("%s: write content for directory %s\n",filename.c_str(),name.c_str());
  GzFileHandler out(filename,filename+"-"+random_base36_string()+".tmp","wb");
  ezwrite(out.get(),content.str().c_str(),content.tellp());
  out.close();
  finish_time=time(NULL);
  info("%s: wrote content for directory %s\n",filename.c_str(),name.c_str());
}


////////////////////////////////////////////////////////////////////////

// Class DirWalkState

DirWalkState::DirWalkState(const string &reldir,const string &path):
  DirResult(), reldir(reldir), path(path), done()
{}

DirWalkState::DirWalkState(gzFile in):
  DirResult(in), reldir(), path(), done()
{
  read_size_string(in,reldir);
  read_size_string(in,path);
  int64_t size;
  ezread(in,(char*)&size,8);
  for(;size>0;size--) {
    string s;
    read_size_string(in,s);
    done.insert(s);
  }
}

void DirWalkState::write_restart(gzFile out) const {
  int64_t size;
  this->DirResult::write_restart(out);
  write_size_string(out,reldir);
  write_size_string(out,path);
  size=done.size();
  ezwrite(out,(const char *)&size,8);
  for(auto it=done.begin();it!=done.end();it++)
    write_size_string(out,*it);
}

////////////////////////////////////////////////////////////////////////

// class DiskUsage: small methods

DiskUsage::DiskUsage(const string &path,const string &restart_path,const string &output_base,bool restart):
  topdir(path), restart_path(restart_path), output_base(output_base),
  have_read_restart(false)
{
  start_time=time(NULL);
  last_restart_write=start_time;
  end_time=-1;

  if(restart) {
    if(!read_restart())
      warning("warning: %s: failed to restart; will rerun instead\n",restart_path.c_str());
    else
      have_read_restart=true;
  }
}

shared_ptr<TopResult> DiskUsage::result_for(const string &name) {
  auto there=hashed_results.find(name);
  if(there!=hashed_results.end() && there->second)
    return there->second;
  shared_ptr<TopResult> newwed(new TopResult(name,""));
  ordered_results.emplace_back(newwed);
  hashed_results[name]=newwed;
  return ordered_results.back();
}

shared_ptr<const TopResult> DiskUsage::result_for(const string &name) const {
  auto there=hashed_results.find(name);
  if(there!=hashed_results.end() && there->second)
    return there->second;
  return nullptr;
}

void DiskUsage::clear() {
  start_time=time(NULL);
  last_restart_write=start_time;
  state.clear();
  ino_seen.clear();
  ordered_results.clear();
  hashed_results.clear();
}

int64_t DiskUsage::time_elapsed() const {
  return end_time-start_time;
}

DirResult DiskUsage::tree_walk() {
  end_time=time(NULL);
  int walk_index=state.empty() ? -1 : 0;
  DirResult result=tree_walk(".",topdir,walk_index,result_for("."),have_read_restart);
  end_time=time(NULL);
  sort_results_by_size();
  return result;
}

bool DiskUsage::should_write_restart() const {
  return time(NULL)-last_restart_write >= g_restart_interval;
}

void DiskUsage::sort_results_by_size() {
  stable_sort(ordered_results.begin(),ordered_results.end(),
              [](const shared_ptr<TopResult> &left,const shared_ptr<TopResult> &right) {
                return right->at_age(0).bytes<left->at_age(0).bytes;
              });
}

////////////////////////////////////////////////////////////////////////

// class DiskUsage: restart files

bool DiskUsage::write_restart() const {
  debug("%s: write restart\n",restart_path.c_str());
  string tempname=restart_path+"-"+random_base36_string()+".tmp";
  try {
    GzFileHandler out(restart_path,tempname,"wb");
    write_restart_impl(out.get());
    out.close();
  } catch(const FileError &fe) {
    error("ERROR: %s: failed to write restart: %s\n",restart_path.c_str(),fe.what());
    return false;
  }
  
  last_restart_write=time(NULL);
  info("%s: (**) wrote RESTART at %s\n",restart_path.c_str(),strstrftime("%a %d %b %Y %T %Z").c_str());
  
  return true;
}

void DiskUsage::write_restart_impl(gzFile out) const {
  int64_t size;

  size=magic_number;
  ezwrite(out,(const char *)(&size),8);
  write_size_string(out,topdir);

  int64_t elapsed=time(NULL)-start_time;
  ezwrite(out,(const char *)(&elapsed),8);

  size=state.size();
  ezwrite(out,(const char *)(&size),8);
  for(auto it=state.begin();it!=state.end();it++)
    it->write_restart(out);

  size=(int64_t)ino_seen.size();
  ezwrite(out,(const char *)&size,8);
  for(auto it=ino_seen.begin();it!=ino_seen.end();it++) {
    size=*it;
    ezwrite(out,(const char *)(&size),8);
  }

  size=(int64_t)ordered_results.size();
  ezwrite(out,(const char *)&size,8);
  for(auto it=ordered_results.begin();it!=ordered_results.end();it++)
    (*it)->write_restart(out);
}

bool DiskUsage::read_restart() {
  try {
    GzFileHandler in(restart_path,"","rb");
    read_restart_impl(in.get());
    in.close();
    return true;
  } catch(const FileError &fe) {
    clear();
    warning("warning: %s: cannot read restart: %s\n",restart_path.c_str(),fe.what());
    return false;
  }
}

void DiskUsage::read_restart_impl(gzFile in) {
  int64_t size;
  ezread(in,(char *)&size,8);
  if(size!=magic_number)
    throw BadFile("wrong magic number");

  string newtop;
  read_size_string(in,newtop);
  if(topdir!=newtop)
    throw BadFile("top dir in restart file ("+newtop+") does not match specified path ("+topdir+")");

  int64_t elapsed;
  ezread(in,(char *)&elapsed,8);

  ezread(in,(char *)&size,8);
  debug("%s: expect %lld states\n",restart_path.c_str(),(long long)size);
  for(;size>0;size--)
    state.emplace_back(in);

  ezread(in,(char *)&size,8);
  debug("%s: expect %lld ino elements\n",restart_path.c_str(),(long long)size);
  for(int64_t idx=0;size>0;idx++,size--) {
    int64_t ino;
    ezread(in,(char *)&ino,8);
    ino_seen.insert((ino_t)ino);
  }
  
  ezread(in,(char *)&size,8);
  for(int64_t idx=0;size>0;idx++,size--) {
    shared_ptr<TopResult> newwed(new TopResult(in));
    ordered_results.emplace_back(newwed);
    hashed_results[newwed->get_name()]=newwed;
  }
  start_time=time(NULL)-elapsed;
}

////////////////////////////////////////////////////////////////////////

// class DiskUsage: tree_walk (the meat of the program)

float DiskUsage::scan_files_and_list_subdirs(
      const string &reldir,const string &path,
      int walk_index,shared_ptr<TopResult> tr,bool restart,
      vector<string> &subdirs) {

  struct dirent *dent;

  time_t last_check = time(NULL);
  size_t files_since_last_check = 0;
  double files_per_second = 0;

  DIR *dir=opendir(path.c_str());
  if(!dir) {
    warning("warning: %s: cannot open dir; will ignore\n",path.c_str());
    state.pop_back();
    return -1;
  }

  while( (dent=readdir(dir)) ) {
    bool skip=false;
    if(restart && state[walk_index].is_done(dent->d_name))
      continue; // Already processed this before restart

    if(dent->d_name[0]=='.') {
      if(dent->d_name[1]=='.' && dent->d_name[2]=='\0')
        continue;
      else if(dent->d_name[1]=='\0')
        skip=true;
    }

    if(ino_seen.find(dent->d_ino)!=ino_seen.end() && !skip) {
      debug("%s/%s: already saw inode number %llu",path.c_str(),dent->d_name,(unsigned long long)dent->d_ino);
      continue; // Don't handle hard links twice
    }

    files_since_last_check++;

    if(dent->d_type == DT_DIR && !skip) {
      subdirs.push_back(dent->d_name);
    } else {
      state[walk_index].mark_done(dent->d_name);
      ino_seen.insert(dent->d_ino);

      struct stat sb;
      if(fstatat(dirfd(dir),dent->d_name,&sb,AT_SYMLINK_NOFOLLOW)) {
        warning("warning: %s/%s: cannot fstatat; will ignore: %s/%s\n",
                reldir.c_str(), path.c_str(), dent->d_name, strerror(errno));
        continue;
      }

      shared_ptr<TopResult> result;
      time_t age = max<time_t>(0,time(NULL) - max(sb.st_mtime,sb.st_atime));
      for(int iage=0; iage<DirResult::age_count; ++iage) {
        if(age > DirResult::age_seconds[iage]) {
          state[walk_index].add(iage, {sb.st_size,sb.st_blocks,1});
          if(walk_index==0 && !skip) {
            // Top level result entry for a file
            if(!result)
              result=result_for(dent->d_name);
            result->add(iage, {sb.st_size,sb.st_blocks,1});
            result->set_finish_time();
          }
          tr->add(iage, {sb.st_size,sb.st_blocks,1});
        }
      }
    }

    time_t now = time(NULL);
    if(now - last_check > g_slow_io_check_interval) {
      files_per_second = files_since_last_check / double(now - last_check);
      info("%s: Check speed. Am here at %f / %f = %f files per second (min allowed %f in %f seconds)\n",
             path.c_str(), double(files_since_last_check), double(now - last_check), files_per_second, g_min_files_per_second, double(g_slow_io_check_interval));
      if(files_per_second < g_min_files_per_second) {
        string host = strhostname();
        warning("warning: %s: Slow scan (%f files per second) at path %s\n",
                host.c_str(), files_per_second, path.c_str());
        warning("warning: %s: Aborting scan of files; proceeding to subdirectories of %s\n", host.c_str(), path.c_str());
        break;
      }
      last_check = now;
      files_since_last_check = 0;
    }

    if(should_write_restart()) {
      if(files_per_second > 0)
        info("%s: am here (1) at %f files per second (min allowed %f)\n",
               path.c_str(), files_per_second, g_min_files_per_second);
      write_restart();
      write_xml_report(g_report_file, *this);
    }
  } // directory loop
  closedir(dir);

  time_t now = time(NULL);
  if(now > last_check) {
    files_per_second = files_since_last_check / double(now - last_check);
    info("%s: Finished files only, now %f / %f = %f files per second\n",
         path.c_str(), double(files_since_last_check), double(now - last_check), files_per_second);
  }

  if(!restart && walk_index==0 && should_write_restart()) {
    if(files_per_second >= 0)
      info("%s: am here (2) at %f files per second (min allowed %f)\n",
             path.c_str(), files_per_second, g_min_files_per_second);
    write_restart();
    write_xml_report(g_report_file, *this);
  }

  return true;
}

////////////////////////////////////////////////////////////////////////

DirResult DiskUsage::tree_walk(const string &reldir,const string &path,
                               int walk_index,shared_ptr<TopResult> tr,bool restart) {
  assert(reldir.find('/') == string::npos);
  if(restart)
    info("restart state[%d] = %s\n",walk_index,path.c_str());
  assert(path.size());

  if(!restart) {
    walk_index=state.size();
    state.emplace_back(reldir,path);
    state.back().clear_done();
  }
  assert(walk_index>=0);

  if(restart) {
    if((int64_t)state.size()>(walk_index+1)) {
      string sub_reldir(state[walk_index+1].get_reldir());
      string sub_path(state[walk_index+1].get_path());
      printf("sub_reldir=%s sub_path=%s\n",sub_reldir.c_str(),sub_path.c_str());
      shared_ptr<TopResult> sub_tr=(walk_index==0) ? result_for(sub_reldir) : tr;
      DirResult result=tree_walk(sub_reldir,sub_path,walk_index+1,sub_tr,restart);
      state[walk_index].mark_done(sub_reldir);
      state[walk_index]+=result;
      if(walk_index==0) {
        string filename = output_base+sub_reldir+".du.gz";
        *tr += result;
        try {
          sub_tr->write_content(filename);
        } catch(const FileError &fe) {
          error("ERROR: %s: %s\n",filename.c_str(),fe.what());
          error("ERROR: Cannot write output files in run area. This is an unrecoverable error.\n");
          error("ERROR: Potential existential crisis: did the disk usage monitor run out of disk space?\n");
          exit(2);
        }
      }
    } else {
      write_restart();
      write_xml_report(g_report_file, *this);
    }
  }

  vector<string> subdirs;
  float scan_rate = scan_files_and_list_subdirs(
      reldir, path,
      walk_index, tr, restart, subdirs);
  if(scan_rate < 0) {
    // Was unable to scan directory. Error already printed.
    // Can't do anything more in this directory, so return an empty scan.
    return DirResult();
  }

  for(auto &subname : subdirs) {
    if(restart && state[walk_index].is_done(subname))
      continue; // Already processed this before restart

    state[walk_index].mark_done(subname);

    string substr = append_path(path, subname);
    shared_ptr<TopResult> sub_tr=(walk_index==0) ? result_for(subname) : tr;
    DirResult result=tree_walk(subname, substr, -1, sub_tr, false);

    state[walk_index]+=result;
    assert(state[walk_index].at_age(2).bytes >= result.at_age(2).bytes);

    if(walk_index==0)
      *tr += result;

    if(should_write_restart()) {
      if(scan_rate > 0)
        info("%s: am here (3) at %f files per second (min allowed %f)\n",
               path.c_str(), scan_rate, g_min_files_per_second);
      write_restart();
      write_xml_report(g_report_file, *this);
    }
  } // subdirectory loop

  DirResult &result = state[walk_index];

  // Update the content for the top-level directory containing this descendant.
  for(auto & fr : result.get_result())
    tr->add_content() << fr.bytes << '\t';
  tr->add_content() << result.at_age(0).files << '\t' << path << endl;
  debug("%lld\t%s\n",(long long)state[walk_index].at_age(0).bytes,path.c_str());

  state.pop_back();

  if(walk_index==1) {
    // Done scanning the top-level directory. Write the directory's report.
    string filename=output_base+reldir+".du.gz";
    try {
      tr->write_content(filename);
    } catch(const FileError &fe) {
      error("ERROR: %s: %s\n",filename.c_str(),fe.what());
      error("ERROR: Cannot write output files in run area. This is an unrecoverable error.\n");
      error("ERROR: Potential existential crisis: did the disk usage monitor run out of disk space?\n");
      exit(2);
    }
  }
  return result;
} // tree_walk

  ////////////////////////////////////////////////////////////////////////

void usage() {
  error("SYNTAX: disk-usage [-f] [-d done_file] [-o report_file] [-m rate] [-r] [-q] [-v] [-t restart_interval] /path/to/dir /path/to/restart.gz /string/prepended/to/output\n"
        "Analyzes disk usage in a project.\n"
        "\n"
        "  -f = if the done_file exists, delete it and run anyway.\n"
        "  -d done_file = write this file upon successful exit. Do not start if this file exists.\n"
        "  -o report_file = write the text report table to this file instead of stdout\n"
        "  -m rate = minimum files per second before skipping a directory's files (real value).\n"
        "  -r = restart from the /path/to/restart.gz file if possible, otherwise start over.\n"
        "  -q = be quiet; only prints errors and warnings.\n"
        "  -v = be extremely verbose; only useful for debugging.\n"
        "  -t restart_interval = write restart files this often (seconds; minimum 10)\n"
        "\n"
        "/path/to/dir = the directory whose usage you want\n"
        "/path/to/restart.gz = name of the gzipped restart file\n"
        "/string/prepended/to/output = prepend this to the name of the gzipped inventory files\n");
}

int main(int argc,char **argv) {
  bool restart=false,force=false;
  string done_file;

  int opt;
  while( (opt=getopt(argc,argv,"fd:o:m:rqvt:")) != -1 ) {
    switch(opt) {
    case 'f':     force=true;                           break;
    case 'd':     done_file=optarg;                     break;
    case 'o':     g_report_file=optarg;                 break;
    case 'm':     g_min_files_per_second=atof(optarg);  break;
    case 'r':     restart=true;                         break;
    case 'q':     be_quiet();                           break;
    case 'v':     be_verbose();                         break;
    case 't':     set_restart_interval(atoi(optarg));   break;
    case 's': set_slow_io_check_interval(atof(optarg)); break;
    default:
      usage();
      return 2;
    }
  }

  if(argc!=optind+3) {
    usage();
    return 2;
  }

  string input_path(argv[optind]);
  string restart_path=strrealpath(argv[optind+1]);
  string output_prepend(argv[optind+2]);

  debug("input_path=\"%s\"=\"%s\" restart_path=\"%s\" output_prepend=\"%s\"\n",
        input_path.c_str(),input_path.c_str(),restart_path.c_str(),output_prepend.c_str());

  struct stat sb;
  if(done_file.size() && !stat(done_file.c_str(),&sb)) {
    if(force) {
      if(unlink(done_file.c_str())) {
        error("ERROR: %s: unable to unlink donefile: %s\n",done_file.c_str(),strerror(errno));
        return 1;
      }
    } else {
      error("ERROR: Donefile \"%s\" already exists. Use -f to force a rerun.\n",done_file.c_str());
      return 0;
    }
  }

  if(restart)
    info("Restart from %s\n",argv[optind+1]);

  DiskUsage du(input_path,restart_path,output_prepend,restart);
  DirResult result=du.tree_walk();
  int64_t elapsed=above_int(du.time_elapsed(),1);

  if(!write_xml_report(g_report_file, du))
    return 1; // already printed error message

  info("Processed %lld files in %lld seconds (%.2f per second)\n",
       (long long)result.at_age(0).files,(long long)elapsed,
       (double)result.at_age(0).files/(double)elapsed);

  if(done_file.size()) {
    ofstream f(done_file.c_str());
    f<<strstrftime("%a %d %b %Y %T %Z")<<endl;
  }

  return 0;
}
