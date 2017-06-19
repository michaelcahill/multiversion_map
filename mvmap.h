/**
 *    Copyright (C) 2017 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects
 *    for all of the code used other than as permitted herein. If you modify
 *    file(s) with this exception, you may extend this exception to your
 *    version of the file(s), but you are not obligated to do so. If you do not
 *    wish to do so, delete this exception statement from your version. If you
 *    delete this exception statement from all source files in the program,
 *    then also delete it in the license file.
 */

#pragma once

#include <cinttypes>
#include <iostream>
#include <map>
#include <boost/optional.hpp>
#include <boost/optional/optional_io.hpp>

namespace mongo {

  template <typename _Key, typename _Tp, typename _TS = std::uint64_t,
            typename _Compare = std::less<_Key>,
            typename _TSCompare = std::less<_TS>,
            typename _Alloc = std::allocator<std::pair<const _Key, _Tp> > >
    class multiversion_map {
    public:
      // Familiar std::map types
      typedef _Key key_type;
      typedef _TS timestamp_type;
      typedef _Tp mapped_type;
      typedef _Compare key_compare;
      typedef _TSCompare timestamp_compare;
      typedef _Alloc allocator_type;

      // The interface we provide is the standard map of key -> value
      // timestamps and tombstones are filtered out
      typedef std::pair<key_type, mapped_type> value_type;

      // What we store internally is a timestamp with every key (the
      // combination is unique, if the application overwrites a key using the
      // same timestamp, behavior is identical to an unversioned map
      typedef std::pair<_Key, _TS> ts_key_type;

      // We also store optional values: a missing value indicates a tombstone
      typedef boost::optional<mapped_type> opmapped_type;

    private:
      class keyts_compare
      : public std::binary_function<ts_key_type, ts_key_type, bool>
      {
        friend class multiversion_map<_Key, _Tp, _TS, _Compare, _TSCompare, _Alloc>;
      protected:
        key_compare _comp;
        timestamp_compare _tscomp;

          keyts_compare(key_compare __c, timestamp_compare __tsc)
          : _comp(__c), _tscomp(__tsc) { }

      public:
        // We only have less than, so first compare keys, if key(x) < key(y), we're done.
        // Otherwise, check for equality with !(key(y) < key(x)) and if keys are equal, compare timestamps.
        // We compare timestamps in reverse order (largest first).
        // That way lower_bound with a (key, timestamp) pair finds the most recent visible version.
        bool operator()(const ts_key_type& __x, const ts_key_type& __y) const {
          return _comp(__x.first, __y.first) ||
                (!_comp(__y.first, __x.first) && _tscomp(__y.second, __x.second));
        }
      };

      key_compare _keycomp;
      timestamp_compare _tscomp;

      timestamp_type _oldest, _current;

      typedef typename _Alloc::template rebind<std::pair<ts_key_type, opmapped_type> >::other _KeyTS_alloc_type;
      typedef std::map<ts_key_type, opmapped_type, keyts_compare, _KeyTS_alloc_type> tsmap_type;
      tsmap_type _tsmap;

      typedef typename _Alloc::template rebind<std::pair<_TS, _Key> >::other _TSKey_alloc_type;
      typedef std::multimap<_TS, ts_key_type, _TSCompare, _TSKey_alloc_type> obsmap_type;
      obsmap_type _obsmap;

      const int kCleansPerUpdate = 2;

      typedef typename tsmap_type::value_type ts_value_type;
      typedef typename tsmap_type::iterator ts_iterator;
      typedef typename tsmap_type::const_iterator const_ts_iterator;
      typedef typename tsmap_type::reverse_iterator reverse_ts_iterator;
      typedef typename tsmap_type::const_reverse_iterator const_reverse_ts_iterator;

      class const_multiversion_iterator {
        friend class multiversion_map<_Key, _Tp, _TS, _Compare, _TSCompare, _Alloc>;
      public:
        explicit
        const_multiversion_iterator(const key_compare &__keycomp,
                              const timestamp_compare &__tscomp,
                              const tsmap_type &__tsmap,
                              timestamp_type __timestamp, const_ts_iterator __tsiter)
        : _keycomp(__keycomp), _tscomp(__tscomp), _tsmap(__tsmap),
          _timestamp(__timestamp), _tsiter(__tsiter) {

          // Make sure we're positioned on a visible record
          if (_tsiter != _tsmap.end()) {
            auto tsval = *_tsiter;
            ts_key_type keyts = tsval.first;
            if (_tscomp(_timestamp, keyts.second) || !_tsiter->second) {
              next(false);
            }
          }
        }

        const_multiversion_iterator(const const_multiversion_iterator &__other)
        : _keycomp(__other._keycomp), _tscomp(__other._tscomp),
          _tsmap(__other._tsmap), _timestamp(__other._timestamp),
          _tsiter(__other._tsiter) { }

        value_type
        operator *() const {
          auto tsval = *_tsiter;
          ts_key_type keyts = tsval.first;
          return value_type(keyts.first, *tsval.second);
        }

        bool
        operator ==(const const_multiversion_iterator& __other) const {
          return _tsiter == __other._tsiter;
        }

        bool
        operator !=(const const_multiversion_iterator& __other) const {
          return !(*this == __other);
        }

        void
        next(bool positioned) {
          boost::optional<key_type> current_key;
          if (positioned)
            current_key = _tsiter->first.first;

          while (++_tsiter != _tsmap.end()) {
            ts_key_type keyts = _tsiter->first;
            // Skip invisible values
            if (_tscomp(_timestamp, keyts.second))
              continue;
            // Don't return tombstones (or any other version of this record)
            if (!_tsiter->second)
              current_key = keyts.first;
            // Stop when we see a new key
            if (!current_key || _tsiter->first.first != *current_key)
              break;
          }
        }

        const_multiversion_iterator&
        operator ++() {
          next(_tsiter != _tsmap.end());
          return *this;
        }

        const_multiversion_iterator&
        operator ++(int) {
          next(_tsiter != _tsmap.end());
          return *this;
        }

      protected:
        const key_compare &_keycomp;
        const timestamp_compare &_tscomp;
        const tsmap_type &_tsmap;
        timestamp_type _timestamp;
        const_ts_iterator _tsiter;
      };
      
      class multiversion_iterator : public const_multiversion_iterator {
      public:
        explicit
        multiversion_iterator(const key_compare &__keycomp,
                              const timestamp_compare &__tscomp,
                              const tsmap_type &__tsmap,
                              timestamp_type __timestamp, ts_iterator __tsiter)
        : const_multiversion_iterator(__keycomp, __tscomp, __tsmap, __timestamp, __tsiter) { }
      };

    public:
      typedef multiversion_iterator iterator;
      typedef const_multiversion_iterator const_iterator;
      typedef typename tsmap_type::size_type size_type;
      typedef typename tsmap_type::difference_type difference_type;

      // Default (empty) constructor
      explicit
      multiversion_map(const key_compare& __comp = key_compare(),
                       const timestamp_compare __tscomp = timestamp_compare(),
                       const allocator_type& __a = allocator_type())
      : _keycomp(__comp), _tscomp(__tscomp),
        _oldest(0), _current(0),
        _tsmap(keyts_compare(__comp, __tscomp), __a), _obsmap(__tscomp, __a)
      { }

      // Update the timestamp window
      void
      set_timestamp(timestamp_type __oldest, timestamp_type __current) {
        _oldest = __oldest;
        _current = __current;
      }

      bool
      has_garbage() const {
        return !_obsmap.empty() && _tscomp(_obsmap.begin().first, _oldest);
      }

      void clean(int n = -1) {
        for(auto i = _obsmap.begin(); i != _obsmap.end() && n != 0; ++i, --n) {
          //std::cout << "clean checking (" << i->second.first << ", " << i->second.second << ")[" << i->first << "] @ " << _oldest << std::endl;
          if (!_tscomp(i->first, _oldest))
            break;
          //std::cout << "erasing (" << i->second.first << ", " << i->second.second << ")[" << i->first << "] @ " << _oldest << std::endl;
          _tsmap.erase(i->second);
        }
      }

      /**
       *  Returns the key comparison object out of which the %map was
       *  constructed.
       */
      key_compare key_comp() const { return _keycomp; }

      iterator begin() {
        return multiversion_iterator(_keycomp, _tscomp, _tsmap, _current,
                                     _tsmap.begin());
      }
      const_iterator end() const {
        return const_multiversion_iterator(_keycomp, _tscomp, _tsmap, _current,
                                     _tsmap.end());
      }

      iterator lower_bound(const key_type& __x)
      {
        return multiversion_iterator(_keycomp, _tscomp, _tsmap, _current,
                                     _tsmap.lower_bound(std::make_pair(__x, _current)));
      }

      iterator upper_bound(const key_type& __x)
      { 
        return multiversion_iterator(_keycomp, _tscomp, _tsmap, _current,
                                     _tsmap.upper_bound(std::make_pair(__x, _current)));
      }

      iterator insert(iterator __position, const value_type& __x) {
        // XXX detect write-write conflicts?  We have __position._timestamp...
        this[__position->first] = __x;
        return __position;
      }

      mapped_type&
      operator[](const key_type& __k)
      {
        clean(kCleansPerUpdate);
        ts_key_type keyts = std::make_pair(__k, _current);
        ts_iterator __i = _tsmap.lower_bound(keyts);
        // NOTE: since we don't know whether our caller is going to update, if
        // there is an existing value, we *always* insert a copy so there is a
        // version with the correct timestamp for modifications
        //
        // __i->first is greater than or equivalent to __k.
        if (__i == _tsmap.end() || _keycomp(__i->first.first, __k) || !__i->second)
          __i = _tsmap.insert(__i, ts_value_type(keyts, mapped_type()));
        else {
          // We're overwriting something, save the old (key, timestamp)
          _obsmap.emplace(_current, __i->first);
          __i = _tsmap.insert(__i, ts_value_type(keyts, *__i->second));
        }
        return *__i->second;
      }

      void erase(const key_type& __k) {
        clean(kCleansPerUpdate);
        ts_key_type keyts(__k, _current);
        ts_iterator __i = _tsmap.lower_bound(keyts);
        if (__i == _tsmap.end() || _keycomp(__i->first.first, __k) || !__i->second)
          return; // nothing to do -- should this throw?
        // We're overwriting something, save the old (key, timestamp)
        _obsmap.emplace(_current, __i->first);
        // Insert a tombstone
        __i = _tsmap.insert(__i, ts_value_type(keyts, {}));
        // Also mark the tombstone so it can be cleaned up when no longer needed
        _obsmap[_current] = __k;
      }

      mapped_type&
      at(const key_type& __k)
      {
        clean(kCleansPerUpdate);
        ts_key_type keyts = std::make_pair(__k, _current);
        ts_iterator __i = _tsmap.lower_bound(keyts);
        if (__i == _tsmap.end() || _keycomp(__i->first.first, __k) || !__i->second)
          throw std::out_of_range("multiversion_map::at");
        else {
          // We're overwriting something, save the old (key, timestamp)
          _obsmap.emplace(_current, __i->first);
          __i = _tsmap.insert(__i, ts_value_type(keyts, *__i->second));
        }
        return *__i->second;
      }

      const mapped_type&
      at(const key_type& __k) const
      {
        // needs a const version of lower_bound
        ts_key_type keyts = std::make_pair(__k, _current);
        const_ts_iterator __i = _tsmap.lower_bound(keyts);
        if (__i == _tsmap.end() || _keycomp(__i->first.first, __k) || !__i->second)
          throw std::out_of_range("multiversion_map::at");
        return *__i->second;
      }
  };

}
