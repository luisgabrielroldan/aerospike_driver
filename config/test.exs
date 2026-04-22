import Config

# Silence happy-path :debug and scripted-failure log noise from the
# driver and its test fakes. Tests that need to assert on a specific log
# message should use `ExUnit.CaptureLog` with an explicit level.
config :logger, level: :warning
