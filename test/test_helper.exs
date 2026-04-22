ExUnit.start(
  exclude: [:integration, :enterprise],
  # Capture log output per-test by default. Passing tests drop their
  # logs; failing tests print the captured logs alongside the failure,
  # so diagnostic signal is preserved. Tests that need to assert on
  # specific log content keep using `ExUnit.CaptureLog.capture_log/1`.
  capture_log: true
)
