## Fix connection stall after large write with backpressure

Sessions could stop processing incoming data after completing a large write that triggered backpressure, causing the connection to hang. Updated the lori dependency to 0.13.1 which fixes the underlying issue.
