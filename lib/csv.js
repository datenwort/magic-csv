var CSV, clone, isArray, isObject, remove,
  indexOf = [].indexOf;

CSV = class CSV {
  constructor(settings1 = {}) {
    var base, base1, base2, base3, base4, base5, base6, base7, base8;
    this.settings = settings1;
    this._init();
    if ((base = this.settings).trim == null) {
      base.trim = true;
    }
    if ((base1 = this.settings).drop_bad_rows == null) {
      base1.drop_bad_rows = false;
    }
    if ((base2 = this.settings).drop_empty_rows == null) {
      base2.drop_empty_rows = true;
    }
    if ((base3 = this.settings).drop_duplicate_rows == null) {
      base3.drop_duplicate_rows = false;
    }
    if ((base4 = this.settings).drop_empty_cols == null) {
      base4.drop_empty_cols = false;
    }
    if ((base5 = this.settings).disable_seek == null) {
      base5.disable_seek = false;
    }
    if ((base6 = this.settings).allow_single_col == null) {
      base6.allow_single_col = false;
    }
    if ((base7 = this.settings).strict_field_count == null) {
      base7.strict_field_count = false;
    }
    if ((base8 = this.settings).default_col_name == null) {
      base8.default_col_name = 'Unknown';
    }
    if (!isArray(this.settings.columns)) {
      this.settings.columns = null;
    }
  }

  _init() {
    this._columns = [];
    this._rows = [];
    this._stats = {
      line_ending: 'unknown',
      delimiter: 'unknown',
      col_count: null,
      row_count: null,
      empty_cols: [],
      duplicate_cols: {},
      bad_row_indexes: [],
      valid_col_count: null,
      blank_col_count: null,
      added_col_count: null,
      dropped_col_count: 0,
      dropped_row_count: 0
    };
    this._blank_cols = [];
    return this._added_cols = [];
  }

  getRaw() {
    return this._raw;
  }

  getStats() {
    return this._stats;
  }

  getColCount() {
    return this._columns.length;
  }

  getRowCount() {
    return this._rows.length;
  }

  getCols() {
    return this._columns;
  }

  getCol(i) {
    var col, k, len1, ref, ref1, row;
    if ((ref = typeof i) !== 'number' && ref !== 'string') {
      return null;
    }
    if (typeof i === 'string') {
      i = this._columns.indexOf(i);
      if (!(i > -1)) {
        return null;
      }
    }
    if (i < 0) {
      i = this.getColCount() + i;
    }
    col = [];
    ref1 = this._rows;
    for (k = 0, len1 = ref1.length; k < len1; k++) {
      row = ref1[k];
      col.push(row[i]);
    }
    return col;
  }

  getRows() {
    return this._rows;
  }

  getRow(i) {
    if (typeof i !== 'number') {
      return null;
    }
    if (this.getRowCount() === 0) {
      return null;
    }
    if (i < 0) {
      i = this.getRowCount() + i;
    }
    if (this._rows[i] == null) {
      return null;
    }
    return this._rows[i];
  }

  getObjects() {
    var i, k, ref, results;
    results = [];
    for (i = k = 0, ref = this.getRowCount(); (0 <= ref ? k < ref : k > ref); i = 0 <= ref ? ++k : --k) {
      results.push(this.getObject(i));
    }
    return results;
  }

  getObject(i) {
    var column, j, k, len1, ob, ref, row;
    row = this.getRow(i);
    if (row == null) {
      return null;
    }
    ob = {};
    ref = this._columns;
    for (j = k = 0, len1 = ref.length; k < len1; j = ++k) {
      column = ref[j];
      if (row[j] != null) {
        ob[column] = row[j];
      }
    }
    return ob;
  }

  toString() {
    var data, k, l, len1, len2, line, ref, row, val;
    if (!(this._columns.length > 0)) {
      return '';
    }
    data = '"' + this._columns.join('","') + '"';
    ref = this._rows;
    for (k = 0, len1 = ref.length; k < len1; k++) {
      row = ref[k];
      line = '\n"';
      for (l = 0, len2 = row.length; l < len2; l++) {
        val = row[l];
        line += val.replace(/\n/g, '\r').replace(/"/g, '""') + '","';
      }
      data += line.slice(0, -2);
    }
    return data;
  }

  writeToStream(stream, callback) {
    return stream.write(this.toString(), function() {
      return stream.end(null, function() {
        return typeof callback === "function" ? callback() : void 0;
      });
    });
  }

  writeToRes(res, filename = 'data.csv', callback) {
    var headers;
    if (typeof res.set === 'function') {
      headers = {
        'Content-Type': 'text/csv',
        'Content-Disposition': `attachment;filename=${filename}`
      };
      res.set(headers);
    }
    return this.writeToStream(res, callback);
  }

  readFile(path, config, callback) {
    var stream;
    this._raw = '';
    return stream = require('fs').createReadStream(path, config).on('data', (chunk) => {
      return this._raw += chunk;
    }).on('error', (err) => {
      return typeof callback === "function" ? callback(this._err('Unable to read ' + path, 'READ')) : void 0;
    }).on('end', () => {
      return this.parse(this._raw, (err, stats) => {
        return typeof callback === "function" ? callback(err, stats) : void 0;
      });
    });
  }

  writeToFile(path, callback) {
    return require('fs').writeFile(path, this.toString(), (err) => {
      if (err != null) {
        return typeof callback === "function" ? callback(this._err('Unable to write ' + path, 'WRITE')) : void 0;
      }
      return typeof callback === "function" ? callback() : void 0;
    });
  }

  readObjects(data, callback) {
    var bestIndex, col, i, k, key, keys, l, len1, len2, len3, len4, m, n, new_val, ob, row, val;
    if (!(isArray(data) && data.length > 0)) {
      return callback(this._err('Input was not an array of objects', 'INPUT'));
    }
    for (i = k = 0, len1 = data.length; k < len1; i = ++k) {
      ob = data[i];
      if (!isObject(ob)) {
        return callback(this._err(`Input at index ${i} was not an object`, 'INPUT'));
      }
    }
    this._init();
    // column index finder
    bestIndex = (cols, col) => {
      var l, len, len2, len3, m, n, ref, ref1, ref2, regex;
      if (this._columns.length === 0) {
        return 0;
      }
      if (col.length > 2) {
        for (len = l = ref = col.length - 2; (ref <= 2 ? l <= 2 : l >= 2); len = ref <= 2 ? ++l : --l) {
          regex = new RegExp('^' + col.substr(0, len));
          ref1 = this._columns.slice().reverse();
          for (i = m = 0, len2 = ref1.length; m < len2; i = ++m) {
            col = ref1[i];
            if (col.match(regex) != null) {
              return -i;
            }
          }
        }
      }
      ref2 = cols.slice().reverse();
      for (i = n = 0, len3 = ref2.length; n < len3; i = ++n) {
        col = ref2[i];
        if (indexOf.call(this._columns, col) < 0) {
          return -i;
        }
      }
    };
    // build columns
    this._columns = Object.keys(data[0]);
    for (l = 0, len2 = data.length; l < len2; l++) {
      ob = data[l];
      keys = Object.keys(ob);
      for (m = 0, len3 = keys.length; m < len3; m++) {
        col = keys[m];
        if (indexOf.call(this._columns, col) >= 0) {
          continue;
        }
        this._columns.splice(bestIndex(keys, col), 0, col);
      }
    }
// build rows
    for (n = 0, len4 = data.length; n < len4; n++) {
      ob = data[n];
      row = [];
      for (key in ob) {
        val = ob[key];
        new_val = '';
        if (isObject(val) || isArray(val)) {
          new_val = JSON.stringify(val);
        } else if (typeof val.toString === 'function') {
          new_val = val.toString();
        }
        row[this._columns.indexOf(key)] = new_val;
      }
      this._rows.push(row);
    }
    // finalize
    return this._finalize(() => {
      this._stats.line_ending = 'n/a';
      this._stats.delimiter = 'n/a';
      return callback();
    });
  }

  parse(data, callback) {
    var allow_row, bad_rows, char, col, col_delimiter, cols, cols_found, count, delimiter, delimiter_types, dup_cols, end, end_index, ending, ends, finish, first_row, getNextColumnName, i, index, j, k, l, len1, len2, len3, len4, len5, len6, line, line_ending, line_index, line_seek_count, m, max_char_count, max_field_count, min_field_count, min_index, n, name, new_col, new_row, newline_flag, o, p, q, quoted, r, ref, ref1, ref2, ref3, ref4, ref5, row, seek, start, start_index, starts, v, val;
    finish = (err) => {
      return typeof callback === "function" ? callback(err, this._stats) : void 0;
    };
    if (typeof data !== 'string') {
      return finish(this._err('Input was not a string', 'INPUT'));
    }
    this._data = data;
    this._init();
    // column name generator
    getNextColumnName = (name = this.settings.default_col_name) => {
      var col_name, i;
      i = indexOf.call(this._columns, name) >= 0 ? 2 : 1;
      while (true) {
        col_name = `${name} ${i++}`;
        if (indexOf.call(this._columns, col_name) < 0) {
          return col_name;
        }
      }
    };
    // detect line ending
    data = data.trim();
    min_index = null;
    ref = {
      'CRLF': '\r\n',
      'LF': '\n',
      'CR': '\r'
    };
    for (name in ref) {
      ending = ref[name];
      i = data.indexOf(ending);
      if (i > 0 && ((min_index == null) || i < min_index)) {
        min_index = i;
        line_ending = ending;
        this._stats.line_ending = name;
      }
    }
    newline_flag = '{{{magic-csv}}}';
    if (line_ending !== '\r\n') {
      data = data.replace(/\r\n/g, newline_flag);
    }
    data = data.split(line_ending);
    if (!(data.length > 1 || (this.settings.columns != null))) {
      return finish(this._err('Line ending detection failed (no rows)'));
    }
    cols = data.shift();
    first_row = cols;
    // detect delimiter
    delimiter_types = {
      ',': 'comma',
      '|': 'pipe',
      '\t': 'tab',
      ';': 'semicolon'
    };
    max_char_count = 0;
    for (char in delimiter_types) {
      name = delimiter_types[char];
      count = cols.split(char).length - 1;
      if (count > max_char_count) {
        delimiter = char;
        max_char_count = count;
      }
    }
    col_delimiter = cols.trim().substr(0, 1) === '"' ? '"' + delimiter + '"' : delimiter;
    cols = cols.split(col_delimiter);
    if (!(cols.length > 1 || this.settings.allow_single_col === true)) {
      return finish(this._err('Delimiter detection failed (no columns)'));
    }
    this._stats.delimiter = cols.length === 1 ? 'n/a' : delimiter_types[delimiter];
    if (this.settings.columns != null) {
      cols = this.settings.columns;
      data.unshift(first_row);
    }
    this._columns = cols;
    // detect columns
    cols_found = [];
    dup_cols = {};
    for (i = k = 0, len1 = cols.length; k < len1; i = ++k) {
      col = cols[i];
      col = col.trim().replace(/^"|"$/g, '').trim();
      if (col === '') {
        cols[i] = getNextColumnName();
        this._blank_cols.push(cols[i]);
      } else {
        if (indexOf.call(cols_found, col) >= 0) {
          new_col = getNextColumnName(col);
          if (dup_cols[col] == null) {
            dup_cols[col] = [];
          }
          dup_cols[col].push(new_col);
          col = new_col;
        }
        cols[i] = col;
        cols_found.push(col);
      }
    }
    this._stats.valid_col_count = cols_found.length;
    this._stats.duplicate_cols = dup_cols;
    if (this._blank_cols.length / cols.length >= .5 && this.settings.allow_single_col !== true) {
      return finish(this._err('Column name detection failed'));
    }
    // parse rows
    bad_rows = [];
    min_field_count = null;
    max_field_count = 0;
    line_seek_count = 0;
    for (line_index = l = 0, len2 = data.length; l < len2; line_index = ++l) {
      line = data[line_index];
      if (line.trim() === '') {
        // parse row
        continue;
      }
      row = line.split(delimiter);
      starts = [];
      ends = [];
      seek = false;
      for (i = m = 0, len3 = row.length; m < len3; i = ++m) {
        val = row[i];
        start = false;
        end = false;
        val = val.replace(/\r/g, '\n').replace(new RegExp(newline_flag, 'g'), '\n');
        v = val.match(/^ "/) != null ? ' ' + val.trim() : val.trim();
        if (this.settings.disable_seek !== true) {
          if (!seek && ((v.match(/^"/) != null) && (v.match(/^""[^"]/) == null)) && ((v.match(/"$/) == null) || (v.match(/[^"]""$/) != null)) && (v.match(/[^"]{1}"[^"]{1}/) == null)) {
            start = true;
            seek = true;
            starts.push(i);
          }
          if (seek && ((v.match(/"$/) != null) && (v.match(/[^"]""$/) == null)) && ((v.match(/^"/) == null) || (v.match(/^""[^"]/) != null))) {
            end = true;
            seek = false;
            ends.push(i);
          }
          if (v === '"' || v === '"""') {
            if (seek) {
              ends.push(i);
            } else {
              starts.push(i);
            }
            seek = !seek;
          }
        }
        quoted = (v.match(/^"/) != null) && (v.match(/"$/) != null);
        if (start || quoted) {
          val = val.replace(/^[\n]*"/, '');
        }
        if (end || quoted) {
          val = val.replace(/"[\n]*$/, '');
        }
        row[i] = val;
      }
      // find terminator
      if (seek || this.settings.strict_field_count === true) {
        if (row.length - cols.length === 1 && row[row.length - 1] === '' && this.settings.strict_field_count !== true) {
          starts.pop();
        } else if (row.length !== cols.length) {
          if (line_seek_count++ > 200) {
            return this._try({
              disable_seek: true
            }, (err) => {
              if (err != null) {
                return finish(this._err('Field terminator not found'));
              }
              return finish();
            });
          }
          if (data[line_index + 1] != null) {
            data[line_index + 1] = line + newline_flag + data[line_index + 1];
          }
          continue;
        }
      }
      line_seek_count = 0;
      // join quoted fields
      if (starts.length > 0 && ends.length > 0) {
        new_row = [];
        index = 0;
        for (i = n = 0, len4 = starts.length; n < len4; i = ++n) {
          start_index = starts[i];
          end_index = ends[i];
          for (j = o = ref1 = index, ref2 = start_index; (ref1 <= ref2 ? o < ref2 : o > ref2); j = ref1 <= ref2 ? ++o : --o) {
            new_row.push(row[j]);
          }
          index = end_index + 1;
          new_row.push(row.slice(start_index, end_index + 1).join(delimiter));
        }
        for (i = p = ref3 = index, ref4 = row.length; (ref3 <= ref4 ? p < ref4 : p > ref4); i = ref3 <= ref4 ? ++p : --p) {
          if (row[i] != null) {
            new_row.push(row[i]);
          }
        }
        row = new_row;
      }
      for (i = q = 0, len5 = row.length; q < len5; i = ++q) {
        val = row[i];
        row[i] = val.replace(/""/g, '"');
      }
      while (row.length > cols.length && row[row.length - 1] === '') {
        row.pop();
      }
      // handle bad row
      allow_row = true;
      if (row.length > cols.length || row.length < cols.length - 2) {
        bad_rows.push(row);
        this._stats.bad_row_indexes.push(line_index);
        if (this.settings.drop_bad_rows === true) {
          allow_row = false;
          this._stats.dropped_row_count++;
        }
      }
      // add row
      if (allow_row) {
        this._rows.push(row);
        if ((max_field_count == null) || row.length < min_field_count) {
          min_field_count = row.length;
        }
        if (row.length > max_field_count) {
          max_field_count = row.length;
        }
      }
    }
    if (max_field_count > 10000) {
      // handle bad rows
      return finish(this._err('Column shifting detected'));
    }
    while (max_field_count > cols.length) {
      col = getNextColumnName();
      this._added_cols.push(col);
      cols.push(col);
    }
    if (this._added_cols.length / cols.length >= .5) {
      return finish(this._err('Column shifting detected'));
    }
    if (bad_rows.length > 0 && this._rows.length === 0) {
      this._stats.dropped_row_count = 0;
      this._stats.bad_row_indexes.length = 0;
      if (this.settings.drop_bad_rows === true) {
        this._rows = bad_rows;
      }
    }
    this._stats.dropped_row_count += line_seek_count;
    if (this.settings.drop_bad_rows !== true && max_field_count > min_field_count) {
      ref5 = this._rows;
      for (r = 0, len6 = ref5.length; r < len6; r++) {
        row = ref5[r];
        while (row.length < max_field_count) {
          row.push('');
        }
      }
    }
    // finalize
    return this._finalize(() => {
      return finish(null, this._stats);
    });
  }

  _finalize(callback) {
    var base, base1, base2, base3, base4, blank, c, col, cols, dup_col, dup_cols, dups, empty, empty_cols, empty_rows, generated, i, j, k, l, len1, len10, len11, len12, len2, len3, len4, len5, len6, len7, len8, len9, m, n, o, p, q, r, ref, ref1, ref2, ref3, ref4, ref5, ref6, ref7, ref8, ref9, row, row_index, rows, s, str, t, u, val, vals, w;
    // standardize rows
    empty_rows = [];
    ref = this._rows;
    for (row_index = k = 0, len1 = ref.length; k < len1; row_index = ++k) {
      row = ref[row_index];
      blank = true;
      for (i = l = 0, len2 = row.length; l < len2; i = ++l) {
        val = row[i];
        if (val == null) {
          val = '';
        }
        if (this.settings.trim === true) {
          val = val.trim();
        }
        row[i] = val;
        if (val.length > 0) {
          blank = false;
        }
      }
      if (blank === true) {
        empty_rows.push(row_index);
      }
      while (row.length < this._columns.length) {
        row.push('');
      }
    }
    // drop empty rows
    if (this.settings.drop_empty_rows === true) {
      ref1 = empty_rows.reverse();
      for (m = 0, len3 = ref1.length; m < len3; m++) {
        i = ref1[m];
        this._stats.dropped_row_count++;
        remove(i, this._rows);
      }
    }
    // reconcile duplicate columns
    dup_cols = [];
    ref2 = this._stats.duplicate_cols;
    for (col in ref2) {
      cols = ref2[col];
      for (n = 0, len4 = cols.length; n < len4; n++) {
        c = cols[n];
        dup_cols.push(c);
      }
      i = this._columns.indexOf(col);
      ref3 = this._rows;
      for (o = 0, len5 = ref3.length; o < len5; o++) {
        row = ref3[o];
        for (p = 0, len6 = cols.length; p < len6; p++) {
          dup_col = cols[p];
          j = this._columns.indexOf(dup_col);
          if (row[j].trim() === '') {
            continue;
          }
          if (row[i].trim() === '' || row[i].trim() === row[j].trim()) {
            row[i] = row[j];
            row[j] = '';
          }
        }
      }
    }
    // find empty columns
    empty_cols = [];
    ref4 = this._columns;
    for (q = 0, len7 = ref4.length; q < len7; q++) {
      col = ref4[q];
      vals = this.getCol(col);
      empty = true;
      for (r = 0, len8 = vals.length; r < len8; r++) {
        val = vals[r];
        if (val.trim() !== '') {
          empty = false;
          break;
        }
      }
      if (empty) {
        empty_cols.push(col);
      }
    }
    ref5 = empty_cols.slice().reverse();
    // drop empty columns
    for (s = 0, len9 = ref5.length; s < len9; s++) {
      col = ref5[s];
      generated = indexOf.call(this._blank_cols, col) >= 0 || indexOf.call(dup_cols, col) >= 0;
      if (!(this.settings.drop_empty_cols === true || generated)) {
        continue;
      }
      if (generated) {
        remove(col, empty_cols, this._blank_cols, dup_cols);
      } else {
        this._stats.dropped_col_count++;
      }
      i = this._columns.indexOf(col);
      remove(i, this._columns);
      remove(i, ...this._rows);
    }
    ref6 = this._stats.duplicate_cols;
    // finalize duplicate columns stat
    for (col in ref6) {
      cols = ref6[col];
      ref7 = cols.slice().reverse();
      for (t = 0, len10 = ref7.length; t < len10; t++) {
        c = ref7[t];
        if (indexOf.call(dup_cols, c) < 0) {
          remove(c, cols);
        }
      }
      if (cols.length === 0) {
        delete this._stats.duplicate_cols[col];
      }
    }
    // stats
    this._stats.empty_cols = empty_cols;
    if ((base = this._stats).col_count == null) {
      base.col_count = this._columns.length;
    }
    if ((base1 = this._stats).row_count == null) {
      base1.row_count = this._rows.length;
    }
    if ((base2 = this._stats).valid_col_count == null) {
      base2.valid_col_count = this._columns.length;
    }
    if ((base3 = this._stats).blank_col_count == null) {
      base3.blank_col_count = this._blank_cols.length;
    }
    if ((base4 = this._stats).added_col_count == null) {
      base4.added_col_count = this._added_cols.length;
    }
    // drop duplicate rows
    if (this.settings.drop_duplicate_rows === true) {
      rows = [];
      dups = [];
      ref8 = this._rows;
      for (i = u = 0, len11 = ref8.length; u < len11; i = ++u) {
        row = ref8[i];
        str = JSON.stringify(row);
        if (indexOf.call(rows, str) >= 0) {
          dups.push(i);
        } else {
          rows.push(str);
        }
      }
      ref9 = dups.reverse();
      for (w = 0, len12 = ref9.length; w < len12; w++) {
        i = ref9[w];
        this._rows.splice(i, 1);
        this._stats.dropped_row_count++;
        this._stats.row_count--;
      }
    }
    // try strict field count
    if (callback == null) {
      return;
    }
    if (this._stats.bad_row_indexes.length === 0) {
      return callback();
    }
    return this._try({
      strict_field_count: true,
      drop_bad_rows: false
    }, function() {
      return callback();
    });
  }

  _load(csv) {
    this._columns = csv._columns;
    this._rows = csv._rows;
    return this._stats = csv._stats;
  }

  _err(msg, code = 'PARSE') {
    var e;
    this._finalize();
    e = new Error(msg);
    e.code = code;
    return e;
  }

  _try(settings = {}, callback) {
    var csv, key, ops, same, val;
    same = true;
    for (key in settings) {
      val = settings[key];
      if (this.settings[key] !== val) {
        same = false;
        break;
      }
    }
    if (same) {
      return callback(true);
    }
    ops = clone(this.settings);
    for (key in settings) {
      val = settings[key];
      ops[key] = val;
    }
    csv = new CSV(ops);
    return csv.parse(this._data, (err) => {
      var stats;
      stats = csv.getStats();
      if ((err == null) && stats.bad_row_indexes.length === 0 && stats.dropped_row_count === 0) {
        this._load(csv);
        return callback(null);
      } else {
        return callback(true);
      }
    });
  }

};

module.exports = CSV;

clone = function(v) {
  return JSON.parse(JSON.stringify(v));
};

isArray = function(v) {
  if (v == null) {
    return false;
  }
  return typeof v === 'object' && v.constructor === Array;
};

isObject = function(v) {
  if (v == null) {
    return false;
  }
  return typeof v === 'object' && v.constructor === Object;
};

remove = function(v, ...arrays) {
  var arr, i, k, len1, results;
  results = [];
  for (k = 0, len1 = arrays.length; k < len1; k++) {
    arr = arrays[k];
    i = typeof v === 'number' ? v : arr.indexOf(v);
    if (i > -1) {
      results.push(arr.splice(i, 1));
    } else {
      results.push(void 0);
    }
  }
  return results;
};
