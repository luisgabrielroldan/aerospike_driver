document.addEventListener("DOMContentLoaded", function () {
  var csrfToken = document
    .querySelector("meta[name='csrf-token']")
    .getAttribute("content");
  var liveSocket = new LiveView.LiveSocket("/live", Phoenix.Socket, {
    params: { _csrf_token: csrfToken },
  });
  liveSocket.connect();
});
