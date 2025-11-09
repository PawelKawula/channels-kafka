<a id="readme-top"></a>

[![Contributors][contributors-shield]][contributors-url]
[![Forks][forks-shield]][forks-url]
[![Stargazers][stars-shield]][stars-url]
[![Issues][issues-shield]][issues-url]
[![MIT][license-shield]][license-url]

<br />
<div align="center">
  <h1>Channels 🔗 Kafka</h1>

  <h3 align="center">channels-kafka</h3>

  <p align="center">
    A Django Channels channel layer that uses Kafka as its backing store.
    <br />
    <a href="https://github.com/PawelKawula/channels-kafka"><strong>Explore the docs »</strong></a>
    <br />
    <br />
    <a href="https://github.com/PawelKawula/channels-kafka/issues/new?labels=bug&template=bug-report---.md">Report Bug</a>
    &middot;
    <a href="https://github.com/PawelKawula/channels-kafka/issues/new?labels=enhancement&template=feature-request---.md">Request Feature</a>
  </p>
</div>

<details>
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a>
      <ul>
        <li><a href="#built-with">Built With</a></li>
      </ul>
    </li>
    <li>
      <a href="#getting-started">Getting Started</a>
      <ul>
        <li><a href="#prerequisites">Prerequisites</a></li>
        <li><a href="#installation">Installation</a></li>
      </ul>
    </li>
    <li><a href="#usage">Usage</a></li>
    <li><a href="#contributing">Contributing</a></li>
    <li><a href="#testing">Testing</a></li>
    <li><a href="#license">License</a></li>
    <li><a href="#contact">Contact</a></li>
  </ol>
</details>

## About The Project

Provides channel layer for django channels using kafka

<p align="right">(<a href="#readme-top">back to top</a>)</p>

### Built With

- [![Django][Django]][Django-url]
- [![Kafka][Kafka]][Kafka-url]

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## Getting Started

To work with django channels using this channel layer you will need to have working kafka cluster. As of time of writing,
this library was only tested on Kafka with Kraft Mode.

### Prerequisites

channels-kafka should be able to be installed using any package manager of your choice, although ci tests
use uv

### Installation

- uv
  ```sh
  uv add channels-kafka
  ```
- pip
  ```sh
  pip install channels-kafka
  ```

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## Usage

Assuming that you already have a working django-channels app using any other layer, the only thing you would need to do in your django
code is to change CHANNEL_LAYERS setting in your django to use this backend with all releavant configuration.

```python
CHANNEL_LAYERS = {
    "default": {
        "BACKEND": "channels_kafka.core.KafkaChannelLayer",
        "CONFIG": {
            "hosts": ["localhost:9092"],
        },
    },
}
```

As for kafka broker, you would need to:

- set **offsets.topic.replication.factor** to **1** since channels documentation says you should be ok with some messages not being delivered to increase throughput
- set **max.in.flight.requests.per.connection** to **1** to ensure that messages are delivered in order

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## Contributing

Contributions are what make the open source community such an amazing place to learn, inspire, and create. Any contributions you make are **greatly appreciated**.

If you have a suggestion that would make this better, please fork the repo and create a pull request. You can also simply open an issue with the tag "enhancement".
Don't forget to give the project a star! Thanks again!

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the Branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## Testing

Check all caps constants in https://github.com/PawelKawula/channels-kafka/blob/master/tests/test_core.py to see which env variables you need to provide to run tests using your kafka cluster and then run

```sh
  pytest
```

<p align="right">(<a href="#readme-top">back to top</a>)</p>

### Top contributors:

<a href="https://github.com/PawelKawula/channels-kafka/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=PawelKawula/channels-kafka" alt="contrib.rocks image" />
</a>

## License

Distributed under the MIT license. See `LICENSE` for more information.

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## Contact

Project Link: [https://github.com/PawelKawula/channels-kafka](https://github.com/PawelKawula/channels-kafka)

<p align="right">(<a href="#readme-top">back to top</a>)</p>

[contributors-shield]: https://img.shields.io/github/contributors/PawelKawula/channels-kafka.svg?style=for-the-badge
[contributors-url]: https://github.com/PawelKawula/channels-kafka/graphs/contributors
[forks-shield]: https://img.shields.io/github/forks/PawelKawula/channels-kafka.svg?style=for-the-badge
[forks-url]: https://github.com/PawelKawula/channels-kafka/network/members
[stars-shield]: https://img.shields.io/github/stars/PawelKawula/channels-kafka.svg?style=for-the-badge
[stars-url]: https://github.com/PawelKawula/channels-kafka/stargazers
[issues-shield]: https://img.shields.io/github/issues/PawelKawula/channels-kafka.svg?style=for-the-badge
[issues-url]: https://github.com/PawelKawula/channels-kafka/issues
[license-shield]: https://img.shields.io/github/license/PawelKawula/channels-kafka.svg?style=for-the-badge
[license-url]: https://github.com/PawelKawula/channels-kafka/blob/master/LICENSE
[product-screenshot]: images/screenshot.png

<!-- Shields.io badges. You can a comprehensive list with many more badges at: https://github.com/inttter/md-badges -->

[Django]: https://img.shields.io/badge/Django-092E20?style=for-the-badge&logo=django&logoColor=green
[Django-url]: https://www.djangoproject.com/
[Kafka]: https://img.shields.io/badge/Apache_Kafka-231F20?style=for-the-badge&logo=apache-kafka&logoColor=white
[Kafka-url]: https://kafka.apache.org/
