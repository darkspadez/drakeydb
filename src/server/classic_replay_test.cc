// Copyright 2026, drakeydb authors.  All rights reserved.
// See LICENSE for licensing terms.

#include "server/classic_replay.h"

#include "base/gtest.h"

namespace dfly {

TEST(ClassicReplayTest, ParseCapaReplyAcceptsOkAndSuffixes) {
  CapaReply plain = ParseCapaReply("OK");
  EXPECT_TRUE(plain.ok);
  EXPECT_FALSE(plain.active_replica);
  EXPECT_FALSE(plain.keydb_fastsync_save);

  CapaReply active = ParseCapaReply("OK active-replica");
  EXPECT_TRUE(active.ok);
  EXPECT_TRUE(active.active_replica);
  EXPECT_FALSE(active.keydb_fastsync_save);

  CapaReply fastsync = ParseCapaReply("OK keydb-fastsync-save");
  EXPECT_TRUE(fastsync.ok);
  EXPECT_FALSE(fastsync.active_replica);
  EXPECT_TRUE(fastsync.keydb_fastsync_save);

  CapaReply both = ParseCapaReply("OK active-replica keydb-fastsync-save");
  EXPECT_TRUE(both.ok);
  EXPECT_TRUE(both.active_replica);
  EXPECT_TRUE(both.keydb_fastsync_save);

  CapaReply spaced = ParseCapaReply("OK   active-replica  keydb-fastsync-save ");
  EXPECT_TRUE(spaced.ok);
  EXPECT_TRUE(spaced.active_replica);
  EXPECT_TRUE(spaced.keydb_fastsync_save);

  // Words this build does not know are ignored, not refused.
  CapaReply unknown = ParseCapaReply("OK some-future-capa active-replica");
  EXPECT_TRUE(unknown.ok);
  EXPECT_TRUE(unknown.active_replica);
  EXPECT_FALSE(unknown.keydb_fastsync_save);
}

TEST(ClassicReplayTest, ParseCapaReplyRejectsNonOk) {
  for (const char* bad : {"OKAY", "ok", "Ok", "ERR", "", " ", " OK", "OKactive-replica",
                          "OK-active-replica", "active-replica OK", "ERR active-replica"}) {
    CapaReply reply = ParseCapaReply(bad);
    EXPECT_FALSE(reply.ok) << bad;
    EXPECT_FALSE(reply.active_replica) << bad;
    EXPECT_FALSE(reply.keydb_fastsync_save) << bad;
  }
}

}  // namespace dfly
