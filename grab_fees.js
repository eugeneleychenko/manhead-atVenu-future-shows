const fetch = (...args) =>
  import("node-fetch").then(({ default: fetch }) => fetch(...args));

fetch("https://artist.atvenu.com/api/private", {
  headers: {
    accept: "application/json",
    "accept-language": "en-US,en;q=0.9",
    "content-type": "application/json",
    "sec-ch-ua":
      '"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "x-atvenu-system": "ShadowClient_edc3986d89",
    "x-csrf-token":
      "/9aVVuvq6xR9QuPKFdjrNQACxLgAIQuQk7PN/NNHXxU3eK8Q6QSFk5gFLxkAX3bhaS/eG2hfrOLoN1SpTOrSEA==",
    cookie:
      "_ga=GA1.1.93359948.1718223235; __stripe_mid=93ff61ad-952a-4032-b3e9-8678c866a3c5b2d841; __stripe_sid=31ff3abc-dc1a-41d0-90f8-45afafacd5278737d5; _ga_K14YM5ZM5C=GS1.1.1718223234.1.1.1718224952.0.0.0; _vfa_service_session=jNiQGpA1f%2BCLfoH6KCUxcpvICdtw5f0T7%2BsRnUXXUP5KXiE%2FSGCISOQgvUDlCnyeN6tKdwzVaOcLZmkPg5VFYntYA5qVju6SMXazUAKflRWK3WbdRulsVKpIMQbf13x%2Fj6wZi3wMXHmCr5px9W3Jd5du0KswmwKnSMWPrc%2FZ1EVqwt1zDdkP%2BJtmbHmMMFTV0nXMfiT10yRUwonO1nxBjGO%2BmPUGSV6zVttaXdYcYMiL8fDHhsndjIJ3h8NRPAabUpye5NG8mvYYT0njidctF4%2FhNp0xvyRAu9NVyOPFKsWXBCFy5V8YRYhB2iFUHGO24IpQ4ReNCCvqY2%2BIZtUuuMPbOJ1rfnkVD5NoULDeEQ9bGAVlwsC%2FG0VjbDRsMuC2PDv7TyHKLCskK5v7F4XWjK5vKXhgJHEbomiU1sJFG%2FkkyDK8pE24bvwKg7MoakWX%2BNGPzM73bIOk6%2BHvc14GgWJkTlAvNquln98l1zYg5H%2BmZT2RZOwEniui0YucRav5%2FPBaohoVw5XzOi9h7PrQ9Gh%2FdrrStnzjKHs3Iod%2BijIaSl9T4SIZGkryzJY3vGbfIsZhNssDJk2CSmn0TvxTx810dNItkVf0hpGe0cyw8FFJwEWSScRQQ8bM8RXagXhBIuEiL70v55Aj6isb7weLqX36oUkNsEQSRPrSNdLMAvPncoaz8j6nbRxxsL8nT4Fk2IkokzRS6ZZJkD%2B%2BwjXuqi9U5nIjroo%2FdJlTC7QmM%2F358DFDFtqRL8ZwO0PD2UTrNL90cdzu08m4pv2ELVdkvEW7DK8n9mO3vHT%2FlGBr8XY4Q6VHLSRL922oSA1%2BQmdK8jfqTkrAG3v26KOAYUBF2tflvf3Z8MXUkDtk9B5mWy62trZR81n%2FHLb9DuKOJZNsgBTWqP8iEgeynqdcn4lmfTNMyBHVZKlkOO%2B66433d8%2B7QgD7pwd2nspLyYXDGx4pO7ro0AA8AOhN8lnEEhLVshRplNA6sW3470gNemE%3D--TFG6QZqxVxDN6jWV--SoIny5BNoZzFlYYAWUJk3A%3D%3D",
    Referer:
      "https://artist.atvenu.com/organizations/3/av/reports/payment_history",
    "Referrer-Policy": "strict-origin-when-cross-origin",
  },
  body: '{"query":"query getPaymentHistoryForOrg($organizationId:ID!,$fromDate:DateTime!,$toDate:DateTime!,$isAdmin:Boolean!){organization(id:$organizationId){journalEntries(fromDate:$fromDate,toDate:$toDate){id timestamp type pendingPayout description currencyFormat{...currencyInfo}amount paymentAmount balance providerAccountId originalAmount originalCurrency accountedById account{name}show{adminShowPayoutPath @include(if:$isAdmin)showName showDate pathToRegisterReports userHasAccess showDeleted}}}}fragment currencyInfo on CurrencyFormat{symbol code isSymbolFirst subunits}","variables":{"organizationId":"3","fromDate":"2024-05-06T04:00:00.000Z","toDate":"2024-06-13T03:59:59.999Z","isAdmin":false},"operationName":"getPaymentHistoryForOrg"}',
  method: "POST",
})
  .then((response) => response.json())
  .then((data) => console.log(data))
  .catch((error) => console.error("Error:", error));
